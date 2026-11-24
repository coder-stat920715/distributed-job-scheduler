package com.example.scheduler.worker;

import com.example.scheduler.worker.lock.DistributedJobLock;
import java.util.UUID;
import org.springframework.stereotype.Component;

/** Separate bean so the AOP proxy applies (self-invocation would bypass the aspect). */
@Component
public class ExclusiveRunner {

  @DistributedJobLock(key = "#jobId")
  public void runExclusive(UUID jobId, Runnable task) {
    task.run();
  }
}
