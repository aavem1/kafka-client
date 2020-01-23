package com.aj.kafka.client;

import org.junit.jupiter.api.Test;

class KafkaClientTemplateTest {

  @Test
  void send() {
    KafkaClientTemplate.readOnlyClient("localhost:9092", "input-topic", "input-group")
        .beanName("bean-name")
        .clientName("clientName")
        .concurrency(2)
        .create();
  }

  @Test
  void sendAsync() {}

  @Test
  void pauseReader() {}

  @Test
  void readAndSend() {}
}
