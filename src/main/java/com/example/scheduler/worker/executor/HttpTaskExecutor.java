package com.example.scheduler.worker.executor;

import com.example.scheduler.domain.JobType;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/** Payload: {"url": "...", "method": "POST", "headers": {"k":"v"}, "body": {...}} */
@Component
public class HttpTaskExecutor implements JobExecutor {

  @Override public JobType supports() { return JobType.HTTP; }

  @Override
  public ExecutionResult execute(ExecutionContext ctx) {
    JsonNode p = ctx.payload();
    SimpleClientHttpRequestFactory f = new SimpleClientHttpRequestFactory();
    f.setConnectTimeout(5_000);
    f.setReadTimeout((int) ctx.timeout().toMillis());

    RestClient.RequestBodySpec spec = RestClient.builder().requestFactory(f).build()
        .method(HttpMethod.valueOf(p.path("method").asText("POST").toUpperCase()))
        .uri(p.get("url").asText())
        .header("Idempotency-Key", ctx.idempotencyKey())
        .header("X-Job-Execution-Id", ctx.executionId().toString());
    p.path("headers").fields().forEachRemaining(e -> spec.header(e.getKey(), e.getValue().asText()));
    if (p.has("body")) spec.contentType(MediaType.APPLICATION_JSON).body(p.get("body").toString());

    // 4xx/5xx raise RestClientResponseException and are treated as failures (retryable).
    String out = spec.retrieve().body(String.class);
    return new ExecutionResult(true, truncate(out));
  }

  private static String truncate(String s) {
    return s == null ? null : s.length() > 4000 ? s.substring(0, 4000) : s;
  }
}
