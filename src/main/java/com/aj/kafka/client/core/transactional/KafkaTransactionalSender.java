package com.aj.kafka.client.core.transactional;

import com.aj.kafka.client.core.KafkaSenderClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public final class KafkaTransactionalSender<K, E> implements KafkaSenderClient<K, E> {
  private String bootstrap;
  private String topic;
  private KafkaTemplate<K, E> kafkaTemplate;

  public KafkaTransactionalSender(String bootstrap, String topic) {
    this.bootstrap = bootstrap;
    this.topic = topic;
    Map<String, Object> config = getProducerConfig();

    final DefaultKafkaProducerFactory<K, E> producerFactory =
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
    config.put(
        ProducerConfig.TRANSACTIONAL_ID_CONFIG,
        transactionalId.toString()); // unique transactional id.
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
  public ListenableFuture<SendResult<K, E>> sendEvent(E event) {
    final ListenableFuture<SendResult<K, E>>[] listenableFuture = new ListenableFuture[1];
    kafkaTemplate.setTransactionIdPrefix("tx.template.override.");
    kafkaTemplate.executeInTransaction(
        operations -> {
          final ListenableFuture<SendResult<K, E>> sendResultListenableFuture =
              kafkaTemplate.send(topic, event);
          listenableFuture[0] = sendResultListenableFuture;
          sendResultListenableFuture.addCallback(
              new ListenableFutureCallback<SendResult<K, E>>() {
                @Override
                public void onSuccess(SendResult<K, E> result) {
                  System.out.println(
                      "Sent Event=["
                          + event
                          + "] with offset=["
                          + result.getRecordMetadata().offset()
                          + "]");
                }

                @Override
                public void onFailure(Throwable ex) {
                  System.out.println(
                      "Unable to send Event=[" + event + "] due to : " + ex.getMessage());
                }
              });
          return true;
        });
    return listenableFuture[0];
  }
}
