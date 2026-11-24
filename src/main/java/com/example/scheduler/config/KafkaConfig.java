package com.example.scheduler.config;

import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration
public class KafkaConfig {

  /** JSON template for internal messages (dispatch, DLT). */
  @Bean
  public KafkaTemplate<String, Object> kafkaTemplate(KafkaProperties props) {
    Map<String, Object> cfg = props.buildProducerProperties(null);
    cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    cfg.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
    return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(cfg));
  }

  /** Raw string template used by the KAFKA job executor to publish user payloads. */
  @Bean
  public KafkaTemplate<String, String> stringKafkaTemplate(KafkaProperties props) {
    Map<String, Object> cfg = props.buildProducerProperties(null);
    cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(cfg));
  }

  @Bean
  public NewTopic dispatchTopic(@Value("${scheduler.topics.dispatch}") String name,
                                @Value("${scheduler.topics.partitions}") int partitions) {
    return TopicBuilder.name(name).partitions(partitions).replicas(1).build();
  }

  @Bean
  public NewTopic dltTopic(@Value("${scheduler.topics.dlt}") String name) {
    return TopicBuilder.name(name).partitions(8).replicas(1).build();
  }
}
