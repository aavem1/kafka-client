package com.aj.kafka.client.core.transactional;

import com.aj.kafka.client.core.KafkaSenderClient;
import com.aj.kafka.client.model.Message;
import com.aj.kafka.client.model.Result;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;

public class KafkaTransactionalSender implements KafkaSenderClient {
  private String bootstrap;
  private String topic;
  private KafkaTemplate kafkaTemplate;

  public KafkaTransactionalSender(String bootstrap, String topic) {
    this.bootstrap = bootstrap;
    this.topic = topic;
    Map<String, Object> config = getProducerConfig();

    final DefaultKafkaProducerFactory<Object, Object> producerFactory =
        new DefaultKafkaProducerFactory<>(config);
    kafkaTemplate = new KafkaTemplate<>(producerFactory);
  }

  private Map<String, Object> getProducerConfig() {
    Map<String, Object> config = new HashMap<>();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

    // transaction properties.
    UUID transactionalId = UUID.randomUUID();
    config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId); // unique transactional id.
    config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    config.put(ProducerConfig.ACKS_CONFIG, "all");
    config.put(ProducerConfig.RETRIES_CONFIG, 3);
    config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
    config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 600000);

    // mandatory
    config.put(ProducerConfig.LINGER_MS_CONFIG, 0);
    config.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);

    return config;
  }

  @Override
  public Result send(Message message) {
    return null;
  }

  @Override
  public Future<Result> sendAsync(Message message) {
    return null;
  }

  @Override
  public List<Future<Result>> sendBatch(List<Message> message) {
    return null;
  }
}
