package com.example.scheduler.engine;

import com.example.scheduler.domain.JobTrigger;
import com.example.scheduler.repository.TriggerRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/** Promotes triggers firing within the horizon from PostgreSQL into the Redis hot index. */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "scheduler.roles.engine", havingValue = "true", matchIfMissing = true)
public class TriggerLoader {

  private final TriggerRepository triggers;
  private final RedisDelayScheduler scheduler;

  @Value("${scheduler.loader.horizon-minutes:10}") private int horizonMinutes;

  @Scheduled(fixedDelayString = "${scheduler.loader.poll-ms:5000}")
  @Transactional
  public void promote() {
    List<JobTrigger> batch;
    int total = 0;
    do {
      batch = triggers.lockDue(Instant.now().plus(Duration.ofMinutes(horizonMinutes)), 1000);
      for (JobTrigger t : batch) {
        scheduler.scheduleFire(t.getId(), t.getNextFireTime());
        t.setPromoted(true);
      }
      triggers.flush();
      total += batch.size();
    } while (batch.size() == 1000);
    if (total > 0) log.debug("promoted {} triggers to redis", total);
  }
}
