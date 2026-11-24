package com.example.scheduler.engine;

import com.example.scheduler.engine.RedisDelayScheduler.DueMember;
import com.example.scheduler.engine.TriggerStateService.Decision;
import com.example.scheduler.messaging.DispatchMessage;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Polls owned slots every ~100ms and dispatches due triggers to Kafka. */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "scheduler.roles.engine", havingValue = "true", matchIfMissing = true)
public class TriggerPicker {

  private final RedisDelayScheduler scheduler;
  private final SlotOwnership slots;
  private final TriggerStateService state;
  private final KafkaTemplate<String, Object> kafka;
  private final MeterRegistry metrics;

  @Value("${scheduler.topics.dispatch}") private String dispatchTopic;
  @Value("${scheduler.picker.batch-size:500}") private int batchSize;
  @Value("${scheduler.picker.lease-seconds:30}") private int leaseSeconds;

  @Scheduled(fixedDelayString = "${scheduler.picker.poll-ms:100}")
  public void poll() {
    for (int slot : slots.ownedSlots()) {
      List<String> due;
      try {
        due = scheduler.popDue(slot, batchSize, Duration.ofSeconds(leaseSeconds));
      } catch (Exception e) {
        log.warn("popDue failed slot={}: {}", slot, e.toString());
        continue;
      }
      if (due == null) continue;
      for (String raw : due) {
        try {
          handle(slot, raw);
        } catch (Exception e) {
          // Member stays in inflight; the reaper re-queues it after the lease expires.
          log.error("dispatch failed member={}", raw, e);
        }
      }
    }
  }

  private void handle(int slot, String raw) {
    DueMember m = DueMember.parse(raw);
    Instant now = Instant.now();
    Decision d = state.decide(m, now);

    if (!d.dispatch()) {                       // stale, paused, or misfire policy says skip
      state.commit(m.triggerId(), m.fireMs(), d.advanceFrom(), d.countDelta());
      scheduler.ack(slot, raw);
      return;
    }
    Instant fire = Instant.ofEpochMilli(m.fireMs());
    metrics.timer("scheduler.dispatch.lag").record(Duration.between(fire, now));

    DispatchMessage msg = new DispatchMessage(UUID.randomUUID(), d.jobId(), m.triggerId(),
        d.tenantId(), fire, m.attempt());
    kafka.send(dispatchTopic, d.tenantId() + ":" + d.jobId(), msg).whenComplete((r, ex) -> {
      if (ex != null) { log.warn("kafka send failed, lease expiry will re-queue: {}", ex.toString()); return; }
      try {
        state.commit(m.triggerId(), m.fireMs(), d.advanceFrom(), d.countDelta());
        scheduler.ack(slot, raw);
      } catch (Exception e) {
        log.error("post-dispatch commit failed member={}", raw, e);   // duplicate is de-duped by claim
      }
    });
  }

  @Scheduled(fixedDelay = 5000)
  public void reap() {
    for (int slot : slots.ownedSlots()) {
      try { scheduler.reap(slot); } catch (Exception e) { log.warn("reap failed slot={}", slot); }
    }
  }
}
