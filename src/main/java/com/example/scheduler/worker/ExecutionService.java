package com.example.scheduler.worker;

import com.example.scheduler.domain.ExecutionStatus;
import com.example.scheduler.domain.JobExecutionAudit;
import com.example.scheduler.messaging.DispatchMessage;
import com.example.scheduler.repository.ExecutionRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class ExecutionService {

  private final ExecutionRepository repo;

  @Value("${scheduler.worker.lease-seconds:30}") private int leaseSeconds;

  /** Empty result means another worker (or an earlier delivery) already claimed this run. */
  @Transactional
  public Optional<JobExecutionAudit> tryClaim(DispatchMessage m, String workerId) {
    Instant now = Instant.now();
    int n = repo.tryClaim(m.executionId(), m.jobId(), m.triggerId(), m.scheduledFireTime(),
        m.attempt(), workerId, now, now.plusSeconds(leaseSeconds));
    return n == 0 ? Optional.empty() : repo.findById(m.executionId());
  }

  public boolean heartbeat(JobExecutionAudit e) {
    Instant now = Instant.now();
    return repo.heartbeat(e.getId(), e.getLeaseVersion(), now, now.plusSeconds(leaseSeconds)) == 1;
  }

  /** @return false if the worker was fenced out (recovery already took the run over). */
  public boolean finish(JobExecutionAudit e, ExecutionStatus status, String output, String error) {
    Instant now = Instant.now();
    long duration = e.getStartedAt() == null ? 0 : Duration.between(e.getStartedAt(), now).toMillis();
    return repo.finish(e.getId(), e.getLeaseVersion(), status.name(), now, duration, error, output) == 1;
  }
}
