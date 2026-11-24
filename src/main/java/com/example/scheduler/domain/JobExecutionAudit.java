package com.example.scheduler.domain;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(name = "job_execution_audit")
@Getter @Setter
public class JobExecutionAudit {
  @Id private UUID id;
  private UUID jobId;
  private UUID triggerId;
  private Instant scheduledFireTime;
  @Enumerated(EnumType.STRING) private ExecutionStatus status;
  private int attempt;
  private long leaseVersion;
  private String workerId;
  private Instant startedAt;
  private Instant finishedAt;
  private Long durationMs;
  private Instant heartbeatAt;
  private Instant leaseUntil;
  @JdbcTypeCode(SqlTypes.JSON) @Column(columnDefinition = "jsonb") private String output;
  private String errorStack;
}
