package com.aj.kafka.client;

import com.aj.kafka.client.commons.KafkaReadSendBuilder;
import com.aj.kafka.client.commons.KafkaReaderBuilder;
import com.aj.kafka.client.commons.KafkaSenderBuilder;
import com.aj.kafka.client.core.KafkaReaderClient;
import com.aj.kafka.client.core.KafkaSenderClient;
import com.aj.kafka.client.handlers.ReadSendTaskHandler;
import com.aj.kafka.client.model.Message;
import com.aj.kafka.client.model.Result;

import javax.naming.OperationNotSupportedException;
import java.util.concurrent.Future;

public final class KafkaClientTemplate {

  private KafkaReaderClient kafkaReaderClient;
  private KafkaSenderClient kafkaSenderClient;

  private KafkaClientTemplate() {}

  public Result send(Message message) {
    return kafkaSenderClient.send(message);
  }

  public Future<Result> sendAsync(Message message)  {
    return kafkaSenderClient.sendAsync(message);
  }

  public void stopReader() {
    kafkaReaderClient.stopReader();
  }

  public void resumeReader()  {
    kafkaReaderClient.resumeReader();
  }

  public void pauseReader()  {
    kafkaReaderClient.pauseReader();
  }

  public void readAndSend(ReadSendTaskHandler taskHandler){

  }

  public static KafkaReaderBuilder readOnlyClient(
      String bootstrap, String topicName, String groupId) {
    return KafkaReaderBuilder.builder(bootstrap, topicName, groupId);
  }

  public static KafkaSenderBuilder sendOnlyClient(String bootstrap, String topicName) {
    return KafkaSenderBuilder.builder(bootstrap, topicName);
  }

  public static KafkaReadSendBuilder transactionalClient(
      String bootstrap, String topicName, String groupId) {
    return KafkaReadSendBuilder.builder(bootstrap, topicName, groupId);
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
