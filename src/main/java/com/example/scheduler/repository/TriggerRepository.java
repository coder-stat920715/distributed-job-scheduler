package com.example.scheduler.repository;

import com.example.scheduler.domain.JobTrigger;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

public interface TriggerRepository extends JpaRepository<JobTrigger, UUID> {

  Optional<JobTrigger> findByJobId(UUID jobId);

  /** Work-partitioning scan: concurrent loaders skip rows another node has locked. */
  @Query(nativeQuery = true, value = """
      SELECT * FROM job_trigger
      WHERE state = 'ACTIVE' AND promoted = FALSE AND next_fire_time IS NOT NULL
        AND next_fire_time < :horizon
      ORDER BY next_fire_time
      LIMIT :n
      FOR UPDATE SKIP LOCKED""")
  List<JobTrigger> lockDue(@Param("horizon") Instant horizon, @Param("n") int n);

  /** Redis-loss safety net: a promoted trigger long past due has lost its ZSET entry. */
  @Modifying
  @Transactional
  @Query(nativeQuery = true, value = """
      UPDATE job_trigger SET promoted = FALSE
      WHERE state = 'ACTIVE' AND promoted = TRUE AND next_fire_time < :cutoff""")
  int resetStalePromoted(@Param("cutoff") Instant cutoff);
}
