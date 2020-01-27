package com.aj.kafka.client.commons;

import com.aj.kafka.client.KafkaClientTemplate;
import com.aj.kafka.client.core.KafkaReaderClient;
import com.aj.kafka.client.core.nontransactional.KafkaNonTransactionalReader;
import com.aj.kafka.client.core.transactional.KafkaTransactionalReader;
import com.aj.kafka.client.handlers.ITaskHandler;
import org.springframework.kafka.listener.AfterRollbackProcessor;

public class KafkaReaderBuilder<E> {
  protected String bootstrap;
  protected String topicName;
  protected boolean transactional;
  protected String beanName;
  protected ITaskHandler<E> iTaskHandler;
  protected int concurrency;
  protected String groupId;
  protected AfterRollbackProcessor failureProcessor;

  public KafkaReaderBuilder(String bootstrap, String topicName, String groupId) {
    this.bootstrap = bootstrap;
    this.topicName = topicName;
    this.groupId = groupId;
  }

  public static KafkaReaderBuilder builder(String bootstrap, String topicName, String groupId) {
    return new KafkaReaderBuilder(bootstrap, topicName, groupId);
  }

  public KafkaReaderBuilder transactional(boolean transactional) {
    this.transactional = transactional;
    return this;
  }

  public KafkaReaderBuilder beanName(String beanName) {
    this.beanName = beanName;
    return this;
  }

  public KafkaReaderBuilder iTaskHandler(ITaskHandler<E> iTaskHandler) {
    this.iTaskHandler = iTaskHandler;
    return this;
  }

  public KafkaReaderBuilder concurrency(int concurrency) {
    this.concurrency = concurrency;
    return this;
  }

  public KafkaReaderBuilder failureProcessor(AfterRollbackProcessor failureProcessor) {
    this.failureProcessor = failureProcessor;
    return this;
  }

  public KafkaClientTemplate<E> create() {
    KafkaReaderClient<E> readerClient;
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
