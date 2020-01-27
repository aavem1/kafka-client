package com.aj.kafka.client.commons;

import com.aj.kafka.client.KafkaClientTemplate;
import com.aj.kafka.client.core.KafkaReaderClient;
import com.aj.kafka.client.core.KafkaSenderClient;
import com.aj.kafka.client.core.transactional.KafkaTransactionalReader;
import com.aj.kafka.client.core.transactional.KafkaTransactionalSender;
import com.aj.kafka.client.handlers.ReadSendTaskHandler;
import org.springframework.kafka.listener.AfterRollbackProcessor;

public final class KafkaReadSendBuilder<E> extends KafkaReaderBuilder {
  private String targetTopicName;

  public KafkaReadSendBuilder(String bootstrap, String topicName, String groupId) {
    super(bootstrap, topicName, groupId);
  }

  public KafkaReadSendBuilder(
      String bootstrap, String topicName, String groupId, String targetTopicName) {
    super(bootstrap, topicName, groupId);
    this.targetTopicName = targetTopicName;
  }

  public KafkaReadSendBuilder iTaskHandler(ReadSendTaskHandler<E> iTaskHandler) {
    this.iTaskHandler = iTaskHandler;
    return this;
  }

  public KafkaReadSendBuilder concurrency(int concurrency) {
    this.concurrency = concurrency;
    return this;
  }

  public KafkaReadSendBuilder failureProcessor(AfterRollbackProcessor failureProcessor) {
    this.failureProcessor = failureProcessor;
    return this;
  }

  public static KafkaReadSendBuilder builder(
      String bootstrap, String sourceTopicName, String targetTopicName, String groupId) {
    return new KafkaReadSendBuilder(bootstrap, sourceTopicName, groupId, targetTopicName);
  }

  public KafkaClientTemplate create() {
    KafkaReaderClient readerClient;
    KafkaSenderClient senderClient;
    readerClient =
        new KafkaTransactionalReader(
            bootstrap, iTaskHandler, topicName, concurrency, beanName, groupId, failureProcessor);
    readerClient.start();
    senderClient = new KafkaTransactionalSender(bootstrap, targetTopicName);
    ((ReadSendTaskHandler) iTaskHandler).setKafkaSenderClient(senderClient);

    return KafkaClientTemplate.KafkaClientTemplateBuilder.aKafkaClientFactory()
        .kafkaReaderClient(readerClient)
        .kafkaSenderClient(senderClient)
        .build();
  }
}
