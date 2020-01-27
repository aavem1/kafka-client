package com.aj.kafka.client;

import com.aj.kafka.client.commons.KafkaReadSendBuilder;
import com.aj.kafka.client.commons.KafkaReaderBuilder;
import com.aj.kafka.client.commons.KafkaSenderBuilder;
import com.aj.kafka.client.core.KafkaReaderClient;
import com.aj.kafka.client.core.KafkaSenderClient;
import com.aj.kafka.client.handlers.ReadSendTaskHandler;

import java.util.Objects;
import java.util.concurrent.ExecutionException;

public final class KafkaClientTemplate<E> {

  private KafkaReaderClient<E> kafkaReaderClient;
  private KafkaSenderClient<String, E> kafkaSenderClient;

  private KafkaClientTemplate() {}

  public static KafkaReaderBuilder readOnlyClient(
      String bootstrap, String topicName, String groupId) {
    return KafkaReaderBuilder.builder(bootstrap, topicName, groupId);
  }

  public static KafkaSenderBuilder sendOnlyClient(String bootstrap, String topicName) {
    return KafkaSenderBuilder.builder(bootstrap, topicName);
  }

  public static KafkaReadSendBuilder transactionalClient(
      String bootstrap, String sourceTopicName, String targetTopicName, String groupId) {
    return KafkaReadSendBuilder.builder(bootstrap, sourceTopicName, targetTopicName, groupId);
  }

  public void send(E event) {
    checkClientTemplateSenderOperations();
    try {
      kafkaSenderClient.send(event);
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void sendAsync(E event) {
    checkClientTemplateSenderOperations();
    kafkaSenderClient.sendAsync(event);
  }

  public void stopReader() {
    checkClientTemplateReaderOperations();
    kafkaReaderClient.stopReader();
  }

  public void resumeReader() {
    checkClientTemplateReaderOperations();
    kafkaReaderClient.resumeReader();
  }

  public void pauseReader() {
    checkClientTemplateReaderOperations();
    kafkaReaderClient.pauseReader();
  }

  private void checkClientTemplateSenderOperations() {
    if (Objects.isNull(kafkaSenderClient)) {
      throw new UnsupportedOperationException(
          "current KafkaClientTemplate doesn't support this operation");
    }
  }

  private void checkClientTemplateReaderOperations() {
    if (Objects.isNull(kafkaSenderClient)) {
      throw new UnsupportedOperationException(
          "current KafkaClientTemplate doesn't support this operation");
    }
  }

  public static final class KafkaClientTemplateBuilder<E> {
    private KafkaReaderClient kafkaReaderClient;
    private KafkaSenderClient kafkaSenderClient;

    private KafkaClientTemplateBuilder() {}

    public static KafkaClientTemplateBuilder aKafkaClientFactory() {
      return new KafkaClientTemplateBuilder();
    }

    public KafkaClientTemplateBuilder kafkaReaderClient(KafkaReaderClient kafkaReaderClient) {
      this.kafkaReaderClient = kafkaReaderClient;
      return this;
    }

    public KafkaClientTemplateBuilder kafkaSenderClient(KafkaSenderClient kafkaSenderClient) {
      this.kafkaSenderClient = kafkaSenderClient;
      return this;
    }

    public KafkaClientTemplate<E> build() {
      KafkaClientTemplate<E> kafkaClientTemplate = new KafkaClientTemplate();
      kafkaClientTemplate.kafkaSenderClient = this.kafkaSenderClient;
      kafkaClientTemplate.kafkaReaderClient = this.kafkaReaderClient;
      return kafkaClientTemplate;
    }
  }
}
