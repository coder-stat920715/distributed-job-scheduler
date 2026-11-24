package com.example.scheduler.engine;

import com.example.scheduler.domain.*;
import com.example.scheduler.engine.RedisDelayScheduler.DueMember;
import com.example.scheduler.repository.JobDefinitionRepository;
import com.example.scheduler.repository.TriggerRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Two-phase firing: decide() is read-only; commit() advances the trigger only AFTER Kafka accepted
 * the dispatch. If the picker dies in between, the reaper re-delivers the same member, and the
 * unique execution key de-duplicates the resulting second dispatch.
 */
@Service
@RequiredArgsConstructor
public class TriggerStateService {

  public record Decision(boolean dispatch, boolean stale, Instant advanceFrom, int countDelta,
                         UUID jobId, String tenantId) {}

  private final TriggerRepository triggers;
  private final JobDefinitionRepository jobs;

  @Transactional(readOnly = true)
  public Decision decide(DueMember m, Instant now) {
    JobTrigger t = triggers.findById(m.triggerId()).orElse(null);
    JobDefinition job = t == null ? null : jobs.findById(t.getJobId()).orElse(null);
    if (t == null || job == null || job.getStatus() == JobStatus.CANCELLED) {
      return new Decision(false, true, null, 0, null, null);
    }
    // Manual fires and retries never advance the schedule.
    if (m.manual() || m.attempt() > 1) {
      return new Decision(true, false, null, 0, job.getId(), job.getTenantId());
    }
    boolean current = t.getNextFireTime() != null
        && t.getNextFireTime().toEpochMilli() == m.fireMs();
    if (t.getState() != TriggerState.ACTIVE || job.getStatus() != JobStatus.ACTIVE || !current) {
      return new Decision(false, true, null, 0, job.getId(), job.getTenantId());   // paused/edited/stale
    }
    Instant fire = Instant.ofEpochMilli(m.fireMs());
    boolean late = Duration.between(fire, now).toMillis() > t.getMisfireThresholdMs();
    UUID jid = job.getId();
    String tenant = job.getTenantId();
    if (!late) return new Decision(true, false, fire, 1, jid, tenant);
    return switch (t.getMisfirePolicy()) {
      case FIRE_NOW -> new Decision(true, false, now, 1, jid, tenant);          // fire once, skip the backlog
      case IGNORE_MISFIRES -> new Decision(true, false, fire, 1, jid, tenant);  // catch up every missed fire
      case SKIP -> new Decision(false, false, now, 1, jid, tenant);
      case RESCHEDULE_NEXT_WITH_EXISTING_COUNT -> new Decision(false, false, now, 0, jid, tenant);
    };
  }

  @Transactional
  public void commit(UUID triggerId, long expectedFireMs, Instant advanceFrom, int countDelta) {
    if (advanceFrom == null) return;
    JobTrigger t = triggers.findById(triggerId).orElse(null);
    if (t == null || t.getNextFireTime() == null || t.getNextFireTime().toEpochMilli() != expectedFireMs) return;
    t.setFireCount(t.getFireCount() + countDelta);
    Instant next = t.computeNext(advanceFrom);
    t.setNextFireTime(next);
    t.setPromoted(false);
    if (next == null) t.setState(TriggerState.COMPLETE);
  }
}
