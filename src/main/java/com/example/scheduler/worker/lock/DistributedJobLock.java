package com.example.scheduler.worker.lock;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface DistributedJobLock {
  /** SpEL over method parameter names, e.g. "#jobId". */
  String key();
  long waitMs() default 0;
  /** Seconds; values <= 0 enable the Redisson watchdog (auto-renewal while the JVM lives). */
  long leaseSeconds() default -1;
}
