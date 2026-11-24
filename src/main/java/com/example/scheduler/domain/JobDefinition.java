package com.example.scheduler.domain;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(name = "job_definition")
@Getter @Setter
public class JobDefinition {
  @Id private UUID id;
  private String tenantId;
  private String name;
  @Enumerated(EnumType.STRING) private JobType jobType;
  @JdbcTypeCode(SqlTypes.JSON) @Column(columnDefinition = "jsonb") private String payload;
  @Enumerated(EnumType.STRING) private JobStatus status;
  private boolean allowOverlap;
  private int timeoutMs;
  private int maxAttempts;
  @Version private Long version;
  private Instant createdAt;
}
