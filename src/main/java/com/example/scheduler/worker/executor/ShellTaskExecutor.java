package com.example.scheduler.worker.executor;

import com.example.scheduler.domain.JobType;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Disabled by default. Payload: {"command": "/usr/bin/backup", "args": ["--full"]}.
 * Only commands on the allow-list may run (no shell interpretation, args passed verbatim).
 */
@Component
@ConditionalOnProperty(name = "scheduler.executors.shell.enabled", havingValue = "true")
public class ShellTaskExecutor implements JobExecutor {

  private final List<String> allowed;

  public ShellTaskExecutor(@Value("${scheduler.executors.shell.allowed-commands:}") List<String> allowed) {
    this.allowed = allowed;
  }

  @Override public JobType supports() { return JobType.SHELL; }

  @Override
  public ExecutionResult execute(ExecutionContext ctx) throws Exception {
    JsonNode p = ctx.payload();
    String command = p.get("command").asText();
    if (!allowed.contains(command)) throw new SecurityException("Command not allow-listed: " + command);
    List<String> cmd = new ArrayList<>();
    cmd.add(command);
    p.path("args").forEach(a -> cmd.add(a.asText()));

    Process proc = new ProcessBuilder(cmd).redirectErrorStream(true).start();
    if (!proc.waitFor(ctx.timeout().toMillis(), TimeUnit.MILLISECONDS)) {
      proc.destroyForcibly();
      throw new IllegalStateException("Process timed out");
    }
    String out = new String(proc.getInputStream().readNBytes(4000), StandardCharsets.UTF_8);
    if (proc.exitValue() != 0) throw new IllegalStateException("Exit code " + proc.exitValue() + ": " + out);
    return new ExecutionResult(true, out);
  }
}
