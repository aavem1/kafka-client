package com.aj.kafka.client.commons;

import com.aj.kafka.client.KafkaClientTemplate;
import com.aj.kafka.client.core.KafkaReaderClient;
import com.aj.kafka.client.core.KafkaSenderClient;
import com.aj.kafka.client.core.nontransactional.KafkaNonTransactionalReader;
import com.aj.kafka.client.core.nontransactional.KafkaNonTransactionalSender;
import com.aj.kafka.client.core.transactional.KafkaTransactionalReader;
import com.aj.kafka.client.core.transactional.KafkaTransactionalSender;

public final class KafkaReadSendBuilder extends KafkaReaderBuilder {

  public static KafkaReadSendBuilder builder(String bootstrap, String topicName, String groupId) {
    return new KafkaReadSendBuilder();
  }

  public KafkaClientTemplate create() {
    KafkaReaderClient readerClient;
    KafkaSenderClient senderClient;
    if (transactional) {
      readerClient =
          new KafkaTransactionalReader(
              bootstrap, iTaskHandler, topicName, concurrency, beanName, groupId, failureProcessor);
      readerClient.start();
      senderClient = new KafkaTransactionalSender(bootstrap);
    } else {
      readerClient =
          new KafkaNonTransactionalReader(
              bootstrap, iTaskHandler, topicName, concurrency, beanName, groupId, failureProcessor);
      readerClient.start();
      senderClient = new KafkaNonTransactionalSender(bootstrap);
    }

    return KafkaClientTemplate.KafkaClientTemplateBuilder.aKafkaClientFactory()
        .kafkaReaderClient(readerClient)
        .kafkaSenderClient(senderClient)
        .build();
  }
}
