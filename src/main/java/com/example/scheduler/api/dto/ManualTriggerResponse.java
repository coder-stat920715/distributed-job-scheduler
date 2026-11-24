package com.example.scheduler.api.dto;

import java.time.Instant;
import java.util.UUID;

public record ManualTriggerResponse(UUID jobId, UUID triggerId, Instant scheduledFor) {}
