package com.example.scheduler.recovery;

import com.example.scheduler.domain.JobDefinition;
import com.example.scheduler.domain.JobExecutionAudit;
import com.example.scheduler.repository.ExecutionRepository;
import com.example.scheduler.repository.JobDefinitionRepository;
import com.example.scheduler.repository.TriggerRepository;
import com.example.scheduler.worker.FailureHandler;
import java.time.Instant;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Safety nets: (1) triggers whose Redis entry was lost get re-promoted; (2) executions whose worker
 * stopped heartbeating are failed (fencing the old worker) and retried or dead-lettered.
 * Per-fire misfire policies are applied in TriggerStateService when the late member is picked.
 */
@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "scheduler.roles.recovery", havingValue = "true", matchIfMissing = true)
public class MisfireRecoveryService {

  private final TriggerRepository triggers;
  private final ExecutionRepository executions;
  private final JobDefinitionRepository jobs;
  private final FailureHandler failures;

  @Scheduled(fixedDelay = 15_000)
  public void repromoteLostTriggers() {
    int n = triggers.resetStalePromoted(Instant.now().minusSeconds(60));
    if (n > 0) log.warn("re-promoting {} triggers whose redis entries were overdue", n);
  }

  @Scheduled(fixedDelay = 10_000)
  @Transactional
  public void reclaimDeadWorkers() {
    Instant now = Instant.now();
    List<JobExecutionAudit> expired = executions.lockExpired(now, 200);
    for (JobExecutionAudit e : expired) {
      if (executions.expire(e.getId(), now) == 0) continue;
      JobDefinition job = jobs.findById(e.getJobId()).orElse(null);
      if (job != null) failures.afterFailure(e, job, "LEASE_EXPIRED");
      log.warn("reclaimed execution {} from dead worker {}", e.getId(), e.getWorkerId());
    }
  }
}
