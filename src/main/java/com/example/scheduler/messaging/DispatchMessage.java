package com.example.scheduler.messaging;

import java.time.Instant;
import java.util.UUID;

public record DispatchMessage(UUID executionId, UUID jobId, UUID triggerId, String tenantId,
                              Instant scheduledFireTime, int attempt) {}
