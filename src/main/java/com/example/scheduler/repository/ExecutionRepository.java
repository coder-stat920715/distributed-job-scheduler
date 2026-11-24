package com.example.scheduler.repository;

import com.example.scheduler.domain.ExecutionStatus;
import com.example.scheduler.domain.JobExecutionAudit;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

public interface ExecutionRepository extends JpaRepository<JobExecutionAudit, UUID> {

  Page<JobExecutionAudit> findByJobId(UUID jobId, Pageable pageable);

  Page<JobExecutionAudit> findByJobIdAndStatus(UUID jobId, ExecutionStatus status, Pageable pageable);

  /** Idempotent claim: returns 0 when this (trigger, fire time, attempt) was already claimed. */
  @Modifying
  @Transactional
  @Query(nativeQuery = true, value = """
      INSERT INTO job_execution_audit
        (id, job_id, trigger_id, scheduled_fire_time, status, attempt, lease_version,
         worker_id, started_at, heartbeat_at, lease_until)
      VALUES (:id, :jobId, :triggerId, :fire, 'RUNNING', :attempt, 0, :worker, :now, :now, :lease)
      ON CONFLICT (trigger_id, scheduled_fire_time, attempt) DO NOTHING""")
  int tryClaim(@Param("id") UUID id, @Param("jobId") UUID jobId, @Param("triggerId") UUID triggerId,
               @Param("fire") Instant fire, @Param("attempt") int attempt, @Param("worker") String worker,
               @Param("now") Instant now, @Param("lease") Instant lease);

  /** Fenced heartbeat: affects 0 rows if recovery has taken the execution over. */
  @Modifying
  @Transactional
  @Query(nativeQuery = true, value = """
      UPDATE job_execution_audit SET heartbeat_at = :now, lease_until = :lease
      WHERE id = :id AND lease_version = :version AND status = 'RUNNING'""")
  int heartbeat(@Param("id") UUID id, @Param("version") long version,
                @Param("now") Instant now, @Param("lease") Instant lease);

  /** Fenced terminal/retry transition. */
  @Modifying
  @Transactional
  @Query(nativeQuery = true, value = """
      UPDATE job_execution_audit
      SET status = :status, finished_at = :now, duration_ms = :duration, error_stack = :err,
          output = CASE WHEN CAST(:out AS text) IS NULL THEN NULL ELSE to_jsonb(CAST(:out AS text)) END
      WHERE id = :id AND lease_version = :version AND status = 'RUNNING'""")
  int finish(@Param("id") UUID id, @Param("version") long version, @Param("status") String status,
             @Param("now") Instant now, @Param("duration") long duration,
             @Param("err") String err, @Param("out") String out);

  @Query(nativeQuery = true, value = """
      SELECT * FROM job_execution_audit
      WHERE status = 'RUNNING' AND lease_until < :now
      ORDER BY lease_until LIMIT :n
      FOR UPDATE SKIP LOCKED""")
  List<JobExecutionAudit> lockExpired(@Param("now") Instant now, @Param("n") int n);

  /** Marks a dead worker's run failed and bumps lease_version so the old worker is fenced out. */
  @Modifying
  @Transactional
  @Query(nativeQuery = true, value = """
      UPDATE job_execution_audit
      SET status = 'FAILED', lease_version = lease_version + 1, finished_at = :now,
          error_stack = 'LEASE_EXPIRED: worker heartbeat stopped'
      WHERE id = :id AND status = 'RUNNING'""")
  int expire(@Param("id") UUID id, @Param("now") Instant now);
}
