package com.example.scheduler.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

/**
 * Redis ZSET hot index. Per slot: sched:{n}:due (score = fire epoch ms) and
 * sched:{n}:inflight (score = lease expiry). The Lua pop moves due members to inflight atomically,
 * so a crash after pop never loses a trigger: the reaper puts expired leases back into due.
 */
@Service
public class RedisDelayScheduler {

  public static final int SLOTS = 64;

  /** Member format: triggerId|fireEpochMs|attempt|A(uto)/M(anual). */
  public record DueMember(UUID triggerId, long fireMs, int attempt, boolean manual) {
    public String encode() { return triggerId + "|" + fireMs + "|" + attempt + "|" + (manual ? "M" : "A"); }

    public static DueMember parse(String s) {
      String[] p = s.split("\\|");
      return new DueMember(UUID.fromString(p[0]), Long.parseLong(p[1]),
          Integer.parseInt(p[2]), "M".equals(p[3]));
    }
  }

  private static final String POP_LUA = """
      local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[3])
      for _, m in ipairs(due) do
        redis.call('ZREM', KEYS[1], m)
        redis.call('ZADD', KEYS[2], ARGV[2], m)
      end
      return due""";

  private static final String REAP_LUA = """
      local exp = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
      for _, m in ipairs(exp) do
        redis.call('ZREM', KEYS[2], m)
        redis.call('ZADD', KEYS[1], ARGV[1], m)
      end
      return #exp""";

  private final StringRedisTemplate redis;
  private final DefaultRedisScript<List> popScript = new DefaultRedisScript<>(POP_LUA, List.class);
  private final DefaultRedisScript<Long> reapScript = new DefaultRedisScript<>(REAP_LUA, Long.class);

  public RedisDelayScheduler(StringRedisTemplate redis) { this.redis = redis; }

  // The {slot} hash tag keeps both keys in one Redis Cluster slot so the Lua script is legal.
  static String due(int slot) { return "sched:{" + slot + "}:due"; }
  static String inflight(int slot) { return "sched:{" + slot + "}:inflight"; }
  public static int slotOf(UUID triggerId) { return Math.floorMod(triggerId.hashCode(), SLOTS); }

  private void add(DueMember m, Instant runAt) {
    redis.opsForZSet().add(due(slotOf(m.triggerId())), m.encode(), runAt.toEpochMilli());
  }

  public void scheduleFire(UUID triggerId, Instant fireAt) {
    add(new DueMember(triggerId, fireAt.toEpochMilli(), 1, false), fireAt);
  }

  public void scheduleRetry(UUID triggerId, Instant originalFire, int attempt, Instant runAt) {
    add(new DueMember(triggerId, originalFire.toEpochMilli(), attempt, false), runAt);
  }

  public Instant scheduleManual(UUID triggerId) {
    Instant now = Instant.now();
    add(new DueMember(triggerId, now.toEpochMilli(), 1, true), now);
    return now;
  }

  public void cancelFire(UUID triggerId, Instant fireAt) {
    DueMember m = new DueMember(triggerId, fireAt.toEpochMilli(), 1, false);
    redis.opsForZSet().remove(due(slotOf(triggerId)), m.encode());
  }

  @SuppressWarnings("unchecked")
  public List<String> popDue(int slot, int batch, Duration lease) {
    long now = System.currentTimeMillis();
    return (List<String>) redis.execute(popScript, List.of(due(slot), inflight(slot)),
        String.valueOf(now), String.valueOf(now + lease.toMillis()), String.valueOf(batch));
  }

  public void ack(int slot, String rawMember) {
    redis.opsForZSet().remove(inflight(slot), rawMember);
  }

  public void reap(int slot) {
    redis.execute(reapScript, List.of(due(slot), inflight(slot)),
        String.valueOf(System.currentTimeMillis()), "500");
  }
}
