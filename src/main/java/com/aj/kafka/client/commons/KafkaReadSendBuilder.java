package com.aj.kafka.client.commons;

import com.aj.kafka.client.KafkaClientTemplate;
import com.aj.kafka.client.core.KafkaReaderClient;
import com.aj.kafka.client.core.KafkaSenderClient;
import com.aj.kafka.client.core.nontransactional.KafkaNonTransactionalReader;
import com.aj.kafka.client.core.nontransactional.KafkaNonTransactionalSender;
import com.aj.kafka.client.core.transactional.KafkaTransactionalReader;
import com.aj.kafka.client.core.transactional.KafkaTransactionalSender;
import com.aj.kafka.client.handlers.ReadSendTaskHandler;

public final class KafkaReadSendBuilder<E> extends KafkaReaderBuilder {
  protected ReadSendTaskHandler taskHandler;
  private String targetTopicName;

  public KafkaReadSendBuilder(String bootstrap, String topicName, String groupId) {
    super(bootstrap, topicName, groupId);
  }

  public KafkaReadSendBuilder(
      String bootstrap, String topicName, String groupId, String targetTopicName) {
    super(bootstrap, topicName, groupId);
    this.targetTopicName = targetTopicName;
  }

  public static KafkaReadSendBuilder builder(
      String bootstrap, String sourceTopicName, String targetTopicName, String groupId) {
    return new KafkaReadSendBuilder(bootstrap, sourceTopicName, groupId, targetTopicName);
  }

  public KafkaClientTemplate create() {
    KafkaReaderClient readerClient;
    KafkaSenderClient senderClient;
    if (transactional) {
      readerClient =
          new KafkaTransactionalReader(
              bootstrap, taskHandler, topicName, concurrency, beanName, groupId, failureProcessor);
      readerClient.start();
      senderClient = new KafkaTransactionalSender(bootstrap, targetTopicName);
    } else {
      readerClient =
          new KafkaNonTransactionalReader<E>(
              bootstrap, iTaskHandler, topicName, concurrency, beanName, groupId, failureProcessor);
      readerClient.start();
      senderClient = new KafkaNonTransactionalSender(bootstrap, targetTopicName);
    }

    return KafkaClientTemplate.KafkaClientTemplateBuilder.aKafkaClientFactory()
        .kafkaReaderClient(readerClient)
        .kafkaSenderClient(senderClient)
        .build();
  }
}
