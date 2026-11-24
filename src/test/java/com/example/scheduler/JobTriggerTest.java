package com.example.scheduler;

import static org.junit.jupiter.api.Assertions.*;

import com.example.scheduler.domain.JobTrigger;
import com.example.scheduler.domain.ScheduleType;
import com.example.scheduler.engine.RedisDelayScheduler.DueMember;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class JobTriggerTest {

  @Test
  void cronComputesNextStrictlyAfter() {
    JobTrigger t = new JobTrigger();
    t.setScheduleType(ScheduleType.CRON);
    t.setCronExpr("0 * * * * *");          // every minute at second 0
    t.setTimezone("UTC");
    assertEquals(Instant.parse("2026-01-01T00:01:00Z"), t.computeNext(Instant.parse("2026-01-01T00:00:30Z")));
    assertEquals(Instant.parse("2026-01-01T00:02:00Z"), t.computeNext(Instant.parse("2026-01-01T00:01:00Z")));
  }

  @Test
  void oneTimeFiresOnceThenExhausts() {
    JobTrigger t = new JobTrigger();
    t.setScheduleType(ScheduleType.ONE_TIME);
    t.setRunAt(Instant.parse("2026-01-01T00:00:10Z"));
    assertEquals(t.getRunAt(), t.computeNext(Instant.parse("2026-01-01T00:00:00Z")));
    assertNull(t.computeNext(t.getRunAt()));
  }

  @Test
  void dueMemberRoundTrips() {
    DueMember m = new DueMember(UUID.randomUUID(), 1_700_000_000_123L, 2, true);
    assertEquals(m, DueMember.parse(m.encode()));
  }
}
