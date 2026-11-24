package com.example.scheduler.common;

import com.example.scheduler.domain.JobType;
import java.util.UUID;

public final class Exceptions {
  private Exceptions() {}

  public static class JobNotFoundException extends RuntimeException {
    public JobNotFoundException(UUID id) { super("Job not found: " + id); }
  }
  public static class InvalidScheduleException extends RuntimeException {
    public InvalidScheduleException(String m) { super(m); }
  }
  public static class JobOverlapException extends RuntimeException {
    public JobOverlapException(String key) { super("Another run holds lock " + key); }
  }
  public static class UnsupportedJobTypeException extends RuntimeException {
    public UnsupportedJobTypeException(JobType t) { super("No executor registered for " + t); }
  }
}
