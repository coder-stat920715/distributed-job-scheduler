package com.example.scheduler.worker.executor;

import com.example.scheduler.domain.JobType;

public interface JobExecutor {
  JobType supports();
  ExecutionResult execute(ExecutionContext ctx) throws Exception;
}
