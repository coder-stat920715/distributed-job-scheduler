package com.example.scheduler.worker;

import com.example.scheduler.common.Exceptions.JobOverlapException;
import com.example.scheduler.domain.ExecutionStatus;
import com.example.scheduler.domain.JobDefinition;
import com.example.scheduler.domain.JobExecutionAudit;
import com.example.scheduler.repository.JobDefinitionRepository;
import com.example.scheduler.worker.executor.ExecutionContext;
import com.example.scheduler.worker.executor.ExecutionResult;
import com.example.scheduler.worker.executor.JobExecutorFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RRateLimiter;
import org.redisson.api.RateIntervalUnit;
import org.redisson.api.RateType;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "scheduler.roles.worker", havingValue = "true", matchIfMissing = true)
public class JobRunner {

  private final JobDefinitionRepository jobs;
  private final JobExecutorFactory factory;
  private final ExecutionService executions;
  private final FailureHandler failures;
  private final ExclusiveRunner exclusive;
  private final ExecutorService jobPool;
  private final ScheduledExecutorService heartbeatPool;
  private final RedissonClient redisson;
  private final MeterRegistry metrics;
  private final ObjectMapper mapper;

  @Value("${scheduler.worker.heartbeat-seconds:5}") private int heartbeatSeconds;
  @Value("${scheduler.tenant.rate-limit-per-second:1000}") private long tenantRate;

  private final Set<String> limiterInitialised = ConcurrentHashMap.newKeySet();

  public void run(JobExecutionAudit exec) {
    JobDefinition job = jobs.findById(exec.getJobId()).orElse(null);
    if (job == null) {
      executions.finish(exec, ExecutionStatus.FAILED, null, "Job no longer exists");
      return;
    }
    try {
      if (job.isAllowOverlap()) execute(exec, job);
      else exclusive.runExclusive(job.getId(), () -> execute(exec, job));
    } catch (JobOverlapException e) {
      // Overlap is disallowed: this fire is skipped, not retried.
      executions.finish(exec, ExecutionStatus.FAILED, null, "SKIPPED_OVERLAP: " + e.getMessage());
    }
  }

  private void execute(JobExecutionAudit exec, JobDefinition job) {
    Timer.Sample sample = Timer.start(metrics);
    ScheduledFuture<?> hb = heartbeatPool.scheduleAtFixedRate(() -> {
      try {
        if (!executions.heartbeat(exec)) log.warn("fenced: heartbeat rejected exec={}", exec.getId());
      } catch (Exception e) { log.warn("heartbeat failed exec={}: {}", exec.getId(), e.toString()); }
    }, heartbeatSeconds, heartbeatSeconds, TimeUnit.SECONDS);

    String status = "success";
    try {
      acquireTenantPermit(job.getTenantId());
      ExecutionContext ctx = new ExecutionContext(exec.getId(), job.getId(), job.getTenantId(),
          job.getJobType(), mapper.readTree(job.getPayload()), exec.getAttempt(),
          Duration.ofMillis(job.getTimeoutMs()), exec.getScheduledFireTime());

      ExecutionResult result = CompletableFuture
          .supplyAsync(() -> call(ctx), jobPool)
          .orTimeout(job.getTimeoutMs(), TimeUnit.MILLISECONDS)
          .join();

      if (!executions.finish(exec, ExecutionStatus.SUCCESS, result.output(), null)) {
        log.warn("fenced: stale worker result discarded exec={}", exec.getId());
      }
    } catch (Exception e) {
      status = "failure";
      String stack = stack(e instanceof CompletionException && e.getCause() != null ? e.getCause() : e);
      boolean retry = failures.willRetry(exec, job);
      if (executions.finish(exec, retry ? ExecutionStatus.RETRYING : ExecutionStatus.FAILED, null, stack)) {
        failures.afterFailure(exec, job, stack);
      } else {
        log.warn("fenced: stale worker failure discarded exec={}", exec.getId());
      }
    } finally {
      hb.cancel(false);
      metrics.counter("scheduler.job.executions", "status", status, "type", job.getJobType().name()).increment();
      sample.stop(metrics.timer("scheduler.job.duration", "type", job.getJobType().name()));
    }
  }

  private ExecutionResult call(ExecutionContext ctx) {
    try {
      return factory.forType(ctx.type()).execute(ctx);
    } catch (Exception e) {
      throw new CompletionException(e);
    }
  }

  private void acquireTenantPermit(String tenant) throws InterruptedException {
    RRateLimiter rl = redisson.getRateLimiter("ratelimit:tenant:" + tenant);
    if (limiterInitialised.add(tenant)) rl.trySetRate(RateType.OVERALL, tenantRate, 1, RateIntervalUnit.SECONDS);
    if (!rl.tryAcquire(1, 5, TimeUnit.SECONDS)) {
      throw new IllegalStateException("Tenant rate limit exceeded: " + tenant);
    }
  }

  private static String stack(Throwable t) {
    StringWriter sw = new StringWriter();
    t.printStackTrace(new PrintWriter(sw));
    String s = sw.toString();
    return s.length() > 8000 ? s.substring(0, 8000) : s;
  }
}
