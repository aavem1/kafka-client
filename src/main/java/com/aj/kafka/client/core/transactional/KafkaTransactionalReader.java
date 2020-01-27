package com.aj.kafka.client.core.transactional;

import com.aj.kafka.client.core.KafkaReaderClient;
import com.aj.kafka.client.handlers.ITaskHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AfterRollbackProcessor;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;

public final class KafkaTransactionalReader<E> implements KafkaReaderClient {
  private String bootstrap;
  private ITaskHandler<E> iTaskHandler;
  private String topicName;
  private int concurrency;
  private String beanName;
  private String groupId;
  private AfterRollbackProcessor failureProcessor;
  private ConcurrentMessageListenerContainer container;

  public KafkaTransactionalReader(
      String bootstrap,
      ITaskHandler<E> iTaskHandler,
      String topicName,
      int concurrency,
      String beanName,
      String groupId,
      AfterRollbackProcessor failureProcessor) {
    this.bootstrap = bootstrap;
    this.iTaskHandler = iTaskHandler;
    this.topicName = topicName;
    this.concurrency = concurrency;
    this.beanName = beanName;
    this.groupId = groupId;
    this.failureProcessor = failureProcessor;
  }

  public void start() {
    ContainerProperties containerProperties = new ContainerProperties(topicName);
    containerProperties.setMessageListener(
        (MessageListener<String, E>) message -> iTaskHandler.onMessage(message.value()));

    final DefaultKafkaConsumerFactory<String, String> consumerFactory =
        new DefaultKafkaConsumerFactory<>(
            consumerConfig(), new StringDeserializer(), new StringDeserializer());
    container = new ConcurrentMessageListenerContainer<>(consumerFactory, containerProperties);
    container.setConcurrency(concurrency);
    container.setBeanName(beanName);
    container.setAfterRollbackProcessor(failureProcessor);
    container.start();
  }

  private Map<String, Object> consumerConfig() {
    Map<String, Object> config = new HashMap<>();
    // server properties
    config.put(BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    // transaction properties.
    config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // common properties
    config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1000);
    config.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1024);
    // de-serializer
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
    return config;
  }

  @Override
  public void stopReader() {
    container.stop();
  }

  @Override
  public void resumeReader() {
    container.resume();
  }

  @Override
  public void pauseReader() {
    container.pause();
  }
}
