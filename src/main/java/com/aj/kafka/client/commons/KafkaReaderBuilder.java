package com.aj.kafka.client.commons;

import com.aj.kafka.client.KafkaClientTemplate;
import com.aj.kafka.client.core.KafkaReaderClient;
import com.aj.kafka.client.core.nontransactional.KafkaNonTransactionalReader;
import com.aj.kafka.client.core.transactional.KafkaTransactionalReader;
import com.aj.kafka.client.handlers.ITaskHandler;
import org.springframework.kafka.listener.AfterRollbackProcessor;

public class KafkaReaderBuilder {
  protected String bootstrap;
  protected String clientName;
  protected String topicName;
  protected boolean transactional;
  protected String beanName;
  protected ITaskHandler iTaskHandler;
  protected int concurrency;
  protected String groupId;
  protected AfterRollbackProcessor failureProcessor;

  public KafkaReaderBuilder() {}

  public static KafkaReaderBuilder builder(String bootstrap, String topicName, String groupId) {
    return new KafkaReaderBuilder();
  }

  public KafkaReaderBuilder clientName(String clientName) {
    this.clientName = clientName;
    return this;
  }

  public KafkaReaderBuilder topicName(String topicName) {
    this.topicName = topicName;
    return this;
  }

  public KafkaReaderBuilder transactional(boolean transactional) {
    this.transactional = transactional;
    return this;
  }

  public KafkaReaderBuilder beanName(String beanName) {
    this.beanName = beanName;
    return this;
  }

  public KafkaReaderBuilder iTaskHandler(ITaskHandler iTaskHandler) {
    this.iTaskHandler = iTaskHandler;
    return this;
  }

  public KafkaReaderBuilder concurrency(int concurrency) {
    this.concurrency = concurrency;
    return this;
  }

  public KafkaReaderBuilder groupId(String groupId) {
    this.groupId = groupId;
    return this;
  }

  public KafkaReaderBuilder failureProcessor(AfterRollbackProcessor failureProcessor) {
    this.failureProcessor = failureProcessor;
    return this;
  }

  public KafkaClientTemplate create() {
    KafkaReaderClient readerClient;
    if (transactional) {
      readerClient =
          new KafkaTransactionalReader(
              bootstrap, iTaskHandler, topicName, concurrency, beanName, groupId, failureProcessor);
      readerClient.start();
    } else {
      readerClient =
          new KafkaNonTransactionalReader(
              bootstrap, iTaskHandler, topicName, concurrency, beanName, groupId, failureProcessor);
      readerClient.start();
    }

    return KafkaClientTemplate.KafkaClientTemplateBuilder.aKafkaClientFactory()
        .kafkaReaderClient(readerClient)
        .build();
  }
}
