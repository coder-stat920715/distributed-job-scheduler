package com.example.scheduler.engine;

import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Lease-based slot ownership so picker nodes poll disjoint slots (no thundering herd).
 * This is an efficiency optimisation only: safety comes from the atomic Lua pop.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "scheduler.roles.engine", havingValue = "true", matchIfMissing = true)
public class SlotOwnership {

  private static final String NODES = "scheduler:nodes";
  private static final Duration LEASE = Duration.ofSeconds(10);

  private final StringRedisTemplate redis;
  private final String nodeId = UUID.randomUUID().toString();
  private volatile List<Integer> owned = List.of();

  public SlotOwnership(StringRedisTemplate redis) { this.redis = redis; }

  public List<Integer> ownedSlots() { return owned; }

  @Scheduled(fixedDelay = 3000, initialDelay = 0)
  public void refresh() {
    try {
      long now = System.currentTimeMillis();
      redis.opsForZSet().add(NODES, nodeId, now);
      redis.opsForZSet().removeRangeByScore(NODES, 0, now - 10_000);
      Long live = redis.opsForZSet().zCard(NODES);
      int cap = (int) Math.ceil(RedisDelayScheduler.SLOTS / (double) Math.max(1L, live == null ? 1 : live));

      List<Integer> mine = new ArrayList<>();
      for (int s = 0; s < RedisDelayScheduler.SLOTS; s++) {
        String key = "scheduler:slotlease:" + s;
        if (nodeId.equals(redis.opsForValue().get(key))) {
          if (mine.size() < cap) { redis.expire(key, LEASE); mine.add(s); }
          else redis.delete(key);              // shed slots when other nodes join
        }
      }
      for (int s = 0; s < RedisDelayScheduler.SLOTS && mine.size() < cap; s++) {
        if (mine.contains(s)) continue;
        Boolean ok = redis.opsForValue().setIfAbsent("scheduler:slotlease:" + s, nodeId, LEASE);
        if (Boolean.TRUE.equals(ok)) mine.add(s);
      }
      owned = List.copyOf(mine);
    } catch (Exception e) {
      log.warn("slot refresh failed: {}", e.toString());
    }
  }

  @PreDestroy
  public void release() {
    for (int s : owned) {
      String key = "scheduler:slotlease:" + s;
      if (nodeId.equals(redis.opsForValue().get(key))) redis.delete(key);
    }
    redis.opsForZSet().remove(NODES, nodeId);
    owned = List.of();
  }
}
