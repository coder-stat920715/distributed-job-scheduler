package com.example.scheduler.worker;

import com.example.scheduler.domain.JobDefinition;
import com.example.scheduler.domain.JobExecutionAudit;
import com.example.scheduler.engine.RedisDelayScheduler;
import com.example.scheduler.messaging.DeadLetter;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** Exponential backoff via the scheduler itself, then Dead Letter Topic when attempts run out. */
@Component
@RequiredArgsConstructor
public class FailureHandler {

  private final RedisDelayScheduler scheduler;
  private final KafkaTemplate<String, Object> kafka;

  @Value("${scheduler.topics.dlt}") private String dltTopic;

  public boolean willRetry(JobExecutionAudit exec, JobDefinition job) {
    return exec.getAttempt() < job.getMaxAttempts();
  }

  /** Call after the failed attempt has been recorded. */
  public void afterFailure(JobExecutionAudit exec, JobDefinition job, String error) {
    if (willRetry(exec, job)) {
      long base = 1000L << Math.min(exec.getAttempt(), 10);
      long delay = Math.min(300_000L, base) + ThreadLocalRandom.current().nextLong(base / 5 + 1);
      scheduler.scheduleRetry(exec.getTriggerId(), exec.getScheduledFireTime(),
          exec.getAttempt() + 1, Instant.now().plusMillis(delay));
    } else {
      kafka.send(dltTopic, exec.getJobId().toString(),
          new DeadLetter(exec.getId(), exec.getJobId(), exec.getAttempt(), error));
    }
  }
}
