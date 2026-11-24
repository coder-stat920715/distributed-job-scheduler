package com.example.scheduler.worker.executor;

import com.example.scheduler.domain.JobType;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** Payload: {"topic": "...", "key": "...", "message": {...}} */
@Component
public class KafkaPublisherTaskExecutor implements JobExecutor {

  private final KafkaTemplate<String, String> kafka;

  public KafkaPublisherTaskExecutor(KafkaTemplate<String, String> stringKafkaTemplate) {
    this.kafka = stringKafkaTemplate;
  }

  @Override public JobType supports() { return JobType.KAFKA; }

  @Override
  public ExecutionResult execute(ExecutionContext ctx) throws Exception {
    JsonNode p = ctx.payload();
    ProducerRecord<String, String> rec = new ProducerRecord<>(
        p.get("topic").asText(), p.hasNonNull("key") ? p.get("key").asText() : null,
        p.get("message").toString());
    rec.headers().add("idempotency-key", ctx.idempotencyKey().getBytes(StandardCharsets.UTF_8));
    var md = kafka.send(rec).get(ctx.timeout().toMillis(), TimeUnit.MILLISECONDS).getRecordMetadata();
    return new ExecutionResult(true, md.topic() + "-" + md.partition() + "@" + md.offset());
  }
}
