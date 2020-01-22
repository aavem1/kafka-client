package com.aj.kafka.client.commons;

import com.aj.kafka.client.KafkaClientTemplate;
import com.aj.kafka.client.core.KafkaSenderClient;
import com.aj.kafka.client.core.nontransactional.KafkaNonTransactionalSender;
import com.aj.kafka.client.core.transactional.KafkaTransactionalSender;

public final class KafkaSenderBuilder {
  private String bootstrap;
  private String clientName;
  private String topicName;
  private boolean transactional;
  private String beanName;

  private KafkaSenderBuilder() {}

  public static KafkaSenderBuilder builder(String bootstrap, String topicName) {
    return new KafkaSenderBuilder();
  }

  public KafkaSenderBuilder clientName(String clientName) {
    this.clientName = clientName;
    return this;
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

  public KafkaClientTemplate create() {
    KafkaSenderClient senderClient;
    if (transactional) {

      senderClient = new KafkaTransactionalSender(bootstrap);
    } else {

      senderClient = new KafkaNonTransactionalSender(bootstrap);
    }

    return KafkaClientTemplate.KafkaClientTemplateBuilder.aKafkaClientFactory()
        .kafkaSenderClient(senderClient)
        .build();
  }
}
