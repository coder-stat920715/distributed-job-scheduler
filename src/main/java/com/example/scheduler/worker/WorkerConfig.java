package com.example.scheduler.worker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "scheduler.roles.worker", havingValue = "true", matchIfMissing = true)
public class WorkerConfig {

  /** Unbounded pool; real concurrency is bounded by the capacity semaphore. */
  @Bean(destroyMethod = "shutdown")
  public ExecutorService jobPool() {
    AtomicInteger n = new AtomicInteger();
    return Executors.newCachedThreadPool(r -> {
      Thread t = new Thread(r, "job-" + n.incrementAndGet());
      t.setDaemon(true);
      return t;
    });
  }

  @Bean
  public Semaphore capacity(@Value("${scheduler.worker.max-concurrent:200}") int max) {
    return new Semaphore(max);
  }

  @Bean(destroyMethod = "shutdown")
  public ScheduledExecutorService heartbeatPool() {
    return Executors.newScheduledThreadPool(2);
  }
}
