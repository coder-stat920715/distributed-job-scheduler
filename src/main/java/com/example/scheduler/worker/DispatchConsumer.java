package com.example.scheduler.worker;

import com.example.scheduler.domain.JobExecutionAudit;
import com.example.scheduler.messaging.DispatchMessage;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Offset commit means "claimed", not "finished". The claim row carries a heartbeat lease, so if
 * this worker dies mid-run the recovery service reclaims and retries the execution.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "scheduler.roles.worker", havingValue = "true", matchIfMissing = true)
public class DispatchConsumer {

  private final ExecutionService executions;
  private final JobRunner runner;
  private final ExecutorService jobPool;
  private final Semaphore capacity;
  private final String workerId = "worker-" + UUID.randomUUID().toString().substring(0, 8);

  public DispatchConsumer(ExecutionService executions, JobRunner runner,
                          ExecutorService jobPool, Semaphore capacity) {
    this.executions = executions;
    this.runner = runner;
    this.jobPool = jobPool;
    this.capacity = capacity;
  }

  @KafkaListener(topics = "${scheduler.topics.dispatch}",
      groupId = "${spring.application.name}-workers",
      concurrency = "${scheduler.worker.listener-concurrency:4}")
  public void onMessage(DispatchMessage msg, Acknowledgment ack) throws InterruptedException {
    capacity.acquire();                       // backpressure: blocks polling while the pool is full
    boolean submitted = false;
    try {
      Optional<JobExecutionAudit> claimed = executions.tryClaim(msg, workerId);
      ack.acknowledge();
      if (claimed.isEmpty()) return;          // duplicate delivery, already claimed
      JobExecutionAudit exec = claimed.get();
      jobPool.execute(() -> {
        try {
          runner.run(exec);
        } catch (Throwable t) {
          log.error("unexpected runner failure exec={}", exec.getId(), t);
        } finally {
          capacity.release();
        }
      });
      submitted = true;
    } finally {
      if (!submitted) capacity.release();
    }
  }
}
