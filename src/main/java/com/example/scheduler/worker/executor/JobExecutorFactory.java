package com.example.scheduler.worker.executor;

import com.example.scheduler.common.Exceptions.UnsupportedJobTypeException;
import com.example.scheduler.domain.JobType;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

@Component
public class JobExecutorFactory {

  private final Map<JobType, JobExecutor> registry;

  public JobExecutorFactory(List<JobExecutor> executors) {
    this.registry = executors.stream()
        .collect(Collectors.toUnmodifiableMap(JobExecutor::supports, Function.identity()));
  }

  public JobExecutor forType(JobType type) {
    JobExecutor e = registry.get(type);
    if (e == null) throw new UnsupportedJobTypeException(type);
    return e;
  }
}
