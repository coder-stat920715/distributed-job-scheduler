package com.example.scheduler.api.dto;

import com.example.scheduler.domain.JobStatus;
import com.example.scheduler.domain.JobType;
import com.example.scheduler.domain.ScheduleType;
import java.time.Instant;
import java.util.UUID;

public record JobResponse(UUID id, String tenantId, String name, JobType jobType, JobStatus status,
                          ScheduleType scheduleType, String cronExpr, Instant nextFireTime) {}
