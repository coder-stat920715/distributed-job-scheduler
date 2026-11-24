package com.example.scheduler.api.dto;

import com.example.scheduler.domain.ExecutionStatus;
import com.example.scheduler.domain.JobExecutionAudit;
import java.time.Instant;
import java.util.UUID;

public record ExecutionView(UUID id, UUID jobId, Instant scheduledFireTime, ExecutionStatus status,
                            int attempt, String workerId, Instant startedAt, Instant finishedAt,
                            Long durationMs, String output, String errorStack) {
  public static ExecutionView of(JobExecutionAudit e) {
    return new ExecutionView(e.getId(), e.getJobId(), e.getScheduledFireTime(), e.getStatus(),
        e.getAttempt(), e.getWorkerId(), e.getStartedAt(), e.getFinishedAt(), e.getDurationMs(),
        e.getOutput(), e.getErrorStack());
  }
}
