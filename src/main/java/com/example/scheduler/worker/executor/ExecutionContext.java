package com.example.scheduler.worker.executor;

import com.example.scheduler.domain.JobType;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

public record ExecutionContext(UUID executionId, UUID jobId, String tenantId, JobType type,
                               JsonNode payload, int attempt, Duration timeout, Instant scheduledFor) {
  /** Stable across retries and duplicate deliveries: downstream systems can de-duplicate on it. */
  public String idempotencyKey() { return jobId + ":" + scheduledFor.toEpochMilli(); }
}
