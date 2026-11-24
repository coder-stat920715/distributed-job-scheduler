package com.example.scheduler.domain;

import jakarta.persistence.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;
import org.springframework.scheduling.support.CronExpression;

@Entity
@Table(name = "job_trigger")
@Getter @Setter
public class JobTrigger {
  @Id private UUID id;
  private UUID jobId;
  @Enumerated(EnumType.STRING) private ScheduleType scheduleType;
  private String cronExpr;
  private String timezone;
  private Instant runAt;
  private Instant nextFireTime;
  @Enumerated(EnumType.STRING) private TriggerState state;
  @Enumerated(EnumType.STRING) private MisfirePolicy misfirePolicy;
  private int misfireThresholdMs;
  private long fireCount;
  private boolean promoted;
  @Version private Long version;

  /** Next fire time strictly after {@code after}; null when the trigger is exhausted. */
  public Instant computeNext(Instant after) {
    if (scheduleType == ScheduleType.ONE_TIME) {
      return runAt != null && after.isBefore(runAt) ? runAt : null;
    }
    ZoneId zone = ZoneId.of(timezone == null ? "UTC" : timezone);
    ZonedDateTime next = CronExpression.parse(cronExpr).next(after.atZone(zone));
    return next == null ? null : next.toInstant();
  }
}
