package com.example.scheduler.api;

import com.example.scheduler.api.dto.*;
import com.example.scheduler.common.Exceptions.InvalidScheduleException;
import com.example.scheduler.common.Exceptions.JobNotFoundException;
import com.example.scheduler.domain.*;
import com.example.scheduler.engine.RedisDelayScheduler;
import com.example.scheduler.repository.ExecutionRepository;
import com.example.scheduler.repository.JobDefinitionRepository;
import com.example.scheduler.repository.TriggerRepository;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class JobService {

  private final JobDefinitionRepository jobs;
  private final TriggerRepository triggers;
  private final ExecutionRepository executions;
  private final RedisDelayScheduler scheduler;

  @Transactional
  public JobResponse create(JobRequest r) {
    Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

    JobDefinition job = new JobDefinition();
    job.setId(UUID.randomUUID());
    job.setTenantId(r.tenantId());
    job.setName(r.name());
    job.setJobType(r.jobType());
    job.setPayload(r.payload().toString());
    job.setStatus(JobStatus.ACTIVE);
    job.setAllowOverlap(Boolean.TRUE.equals(r.allowOverlap()));
    job.setTimeoutMs(r.timeoutMs() == null ? 30_000 : r.timeoutMs());
    job.setMaxAttempts(r.maxAttempts() == null ? 3 : r.maxAttempts());
    job.setCreatedAt(now);

    JobTrigger t = new JobTrigger();
    t.setId(UUID.randomUUID());
    t.setJobId(job.getId());
    t.setScheduleType(r.scheduleType());
    t.setState(TriggerState.ACTIVE);
    t.setMisfirePolicy(r.misfirePolicy() == null ? MisfirePolicy.FIRE_NOW : r.misfirePolicy());
    t.setMisfireThresholdMs(5000);
    applySchedule(t, r.cronExpr(), r.timezone(), r.runAt(), r.delaySeconds(), now);
    t.setNextFireTime(initialNext(t, now));
    t.setPromoted(false);

    jobs.save(job);
    triggers.save(t);
    return toResponse(job, t);
  }

  @Transactional(readOnly = true)
  public JobResponse get(UUID id) {
    JobDefinition job = job(id);
    return toResponse(job, trigger(id));
  }

  @Transactional
  public JobResponse update(UUID id, UpdateJobRequest u) {
    Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    JobDefinition job = job(id);
    JobTrigger t = trigger(id);
    evict(t);

    if (u.payload() != null) job.setPayload(u.payload().toString());
    if (u.allowOverlap() != null) job.setAllowOverlap(u.allowOverlap());
    if (u.timeoutMs() != null) job.setTimeoutMs(u.timeoutMs());
    if (u.maxAttempts() != null) job.setMaxAttempts(u.maxAttempts());
    if (u.misfirePolicy() != null) t.setMisfirePolicy(u.misfirePolicy());

    boolean scheduleChanged = u.cronExpr() != null || u.timezone() != null || u.runAt() != null;
    if (scheduleChanged) {
      applySchedule(t, u.cronExpr() != null ? u.cronExpr() : t.getCronExpr(),
          u.timezone() != null ? u.timezone() : t.getTimezone(),
          u.runAt() != null ? u.runAt() : t.getRunAt(), null, now);
      if (t.getState() == TriggerState.ACTIVE) t.setNextFireTime(initialNext(t, now));
    }
    t.setPromoted(false);    // loader re-promotes the (possibly new) next fire time
    return toResponse(job, t);
  }

  @Transactional
  public JobResponse pause(UUID id) {
    JobDefinition job = job(id);
    JobTrigger t = trigger(id);
    job.setStatus(JobStatus.PAUSED);
    t.setState(TriggerState.PAUSED);
    evict(t);
    return toResponse(job, t);
  }

  @Transactional
  public JobResponse resume(UUID id) {
    Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    JobDefinition job = job(id);
    JobTrigger t = trigger(id);
    if (job.getStatus() == JobStatus.CANCELLED) throw new InvalidScheduleException("Cancelled jobs cannot be resumed");
    job.setStatus(JobStatus.ACTIVE);
    t.setState(TriggerState.ACTIVE);
    t.setNextFireTime(initialNext(t, now));
    t.setPromoted(false);
    return toResponse(job, t);
  }

  @Transactional
  public void cancel(UUID id) {
    JobDefinition job = job(id);
    JobTrigger t = trigger(id);
    job.setStatus(JobStatus.CANCELLED);
    t.setState(TriggerState.CANCELLED);
    evict(t);
  }

  @Transactional(readOnly = true)
  public ManualTriggerResponse triggerNow(UUID id) {
    JobDefinition job = job(id);
    if (job.getStatus() == JobStatus.CANCELLED) throw new InvalidScheduleException("Job is cancelled");
    JobTrigger t = trigger(id);
    Instant when = scheduler.scheduleManual(t.getId());
    return new ManualTriggerResponse(id, t.getId(), when);
  }

  @Transactional(readOnly = true)
  public Page<ExecutionView> history(UUID id, ExecutionStatus status, Pageable page) {
    job(id);
    Page<JobExecutionAudit> p = status == null
        ? executions.findByJobId(id, page) : executions.findByJobIdAndStatus(id, status, page);
    return p.map(ExecutionView::of);
  }

  // ---- helpers ----

  private void applySchedule(JobTrigger t, String cron, String tz, Instant runAt, Long delaySeconds, Instant now) {
    String zone = tz == null ? "UTC" : tz;
    try { ZoneId.of(zone); } catch (Exception e) { throw new InvalidScheduleException("Unknown timezone: " + zone); }
    t.setTimezone(zone);
    if (t.getScheduleType() == ScheduleType.CRON) {
      if (cron == null || !CronExpression.isValidExpression(cron)) {
        throw new InvalidScheduleException("Invalid cron expression (6 fields required): " + cron);
      }
      t.setCronExpr(cron);
    } else {
      Instant at = delaySeconds != null ? now.plusSeconds(delaySeconds) : runAt;
      if (at == null) throw new InvalidScheduleException("ONE_TIME jobs need runAt or delaySeconds");
      t.setRunAt(at.truncatedTo(ChronoUnit.MILLIS));
    }
  }

  private Instant initialNext(JobTrigger t, Instant now) {
    if (t.getScheduleType() == ScheduleType.CRON) return t.computeNext(now);
    return t.getRunAt().isAfter(now) ? t.getRunAt() : now;    // overdue one-time jobs fire immediately
  }

  private void evict(JobTrigger t) {
    if (t.getNextFireTime() != null) scheduler.cancelFire(t.getId(), t.getNextFireTime());
  }

  private JobDefinition job(UUID id) { return jobs.findById(id).orElseThrow(() -> new JobNotFoundException(id)); }

  private JobTrigger trigger(UUID jobId) { return triggers.findByJobId(jobId).orElseThrow(() -> new JobNotFoundException(jobId)); }

  private JobResponse toResponse(JobDefinition j, JobTrigger t) {
    return new JobResponse(j.getId(), j.getTenantId(), j.getName(), j.getJobType(), j.getStatus(),
        t.getScheduleType(), t.getCronExpr(), t.getNextFireTime());
  }
}
