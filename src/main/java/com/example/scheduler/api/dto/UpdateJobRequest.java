package com.example.scheduler.api.dto;

import com.example.scheduler.domain.MisfirePolicy;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.time.Instant;

/** All fields optional; only non-null fields are applied. */
public record UpdateJobRequest(
    String cronExpr, String timezone, Instant runAt, JsonNode payload,
    MisfirePolicy misfirePolicy, Boolean allowOverlap,
    @Min(100) @Max(3_600_000) Integer timeoutMs,
    @Min(1) @Max(20) Integer maxAttempts) {}
