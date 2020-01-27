package com.aj.kafka.client.commons;

import com.aj.kafka.client.KafkaClientTemplate;
import com.aj.kafka.client.core.KafkaSenderClient;
import com.aj.kafka.client.core.nontransactional.KafkaNonTransactionalSender;
import com.aj.kafka.client.core.transactional.KafkaTransactionalSender;

public final class KafkaSenderBuilder<E> {
  private String bootstrap;
  private String topicName;
  private boolean transactional;
  private String beanName;

  private KafkaSenderBuilder() {}

  public KafkaSenderBuilder(String bootstrap, String topicName) {
    this.bootstrap = bootstrap;
    this.topicName = topicName;
  }

  public static KafkaSenderBuilder builder(String bootstrap, String topicName) {
    return new KafkaSenderBuilder(bootstrap, topicName);
  }

  public KafkaSenderBuilder topicName(String topicName) {
    this.topicName = topicName;
    return this;
  }

  public KafkaSenderBuilder transactional(boolean transactional) {
    this.transactional = transactional;
    return this;
  }

  public KafkaSenderBuilder beanName(String beanName) {
    this.beanName = beanName;
    return this;
  }

  public KafkaClientTemplate<E> create() {
    KafkaSenderClient<String, E> senderClient;
    if (transactional) {
      senderClient = new KafkaTransactionalSender(bootstrap, topicName);
    } else {
      senderClient = new KafkaNonTransactionalSender(bootstrap, topicName);
    }

    return KafkaClientTemplate.KafkaClientTemplateBuilder.aKafkaClientFactory()
        .kafkaSenderClient(senderClient)
        .build();
  }
}
