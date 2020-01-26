package com.aj.kafka.client;

import com.aj.kafka.client.commons.KafkaReadSendBuilder;
import com.aj.kafka.client.commons.KafkaReaderBuilder;
import com.aj.kafka.client.commons.KafkaSenderBuilder;
import com.aj.kafka.client.core.KafkaReaderClient;
import com.aj.kafka.client.core.KafkaSenderClient;
import com.aj.kafka.client.handlers.ReadSendTaskHandler;

import java.util.concurrent.ExecutionException;

public final class KafkaClientTemplate<E> {

  private KafkaReaderClient kafkaReaderClient;
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
    try {
      kafkaSenderClient.send(event);
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void sendAsync(E event) {
    kafkaSenderClient.sendAsync(event);
  }

  public void stopReader() {
    kafkaReaderClient.stopReader();
  }

  public void resumeReader() {
    kafkaReaderClient.resumeReader();
  }

  public void pauseReader() {
    kafkaReaderClient.pauseReader();
  }

  public <NE> void readAndSend(ReadSendTaskHandler<E, NE> readSendTaskHandler) {
    readSendTaskHandler.setKafkaSenderClient(kafkaSenderClient);
  }

  public static final class KafkaClientTemplateBuilder {
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

    public KafkaClientTemplate build() {
      KafkaClientTemplate kafkaClientTemplate = new KafkaClientTemplate();
      kafkaClientTemplate.kafkaSenderClient = this.kafkaSenderClient;
      kafkaClientTemplate.kafkaReaderClient = this.kafkaReaderClient;
      return kafkaClientTemplate;
    }
  }
}
