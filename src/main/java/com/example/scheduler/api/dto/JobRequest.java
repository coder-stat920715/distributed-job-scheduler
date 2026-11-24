package com.example.scheduler.api.dto;

import com.example.scheduler.domain.JobType;
import com.example.scheduler.domain.MisfirePolicy;
import com.example.scheduler.domain.ScheduleType;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;

/**
 * CRON jobs need cronExpr (Spring 6-field format: sec min hour dom month dow).
 * ONE_TIME jobs need runAt or delaySeconds.
 */
public record JobRequest(
    @NotBlank String tenantId,
    @NotBlank String name,
    @NotNull JobType jobType,
    @NotNull ScheduleType scheduleType,
    String cronExpr,
    String timezone,
    Instant runAt,
    @Min(0) Long delaySeconds,
    @NotNull JsonNode payload,
    MisfirePolicy misfirePolicy,
    Boolean allowOverlap,
    @Min(100) @Max(3_600_000) Integer timeoutMs,
    @Min(1) @Max(20) Integer maxAttempts) {}
