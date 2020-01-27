package com.aj.kafka.client;

import org.junit.jupiter.api.Test;

class KafkaClientTemplateTest {

  @Test
  void send() {

    final KafkaClientTemplate<String> kafkaClient =
        KafkaClientTemplate.sendOnlyClient("localhost:9092", "input-topic")
            .beanName("bean-name")
            .create();
    kafkaClient.send("test");
  }

  @Test
  void sendAsync() {
    final KafkaClientTemplate<String> kafkaClient =
        KafkaClientTemplate.sendOnlyClient("localhost:9092", "input-topic")
            .beanName("bean-name")
            .create();
    kafkaClient.sendAsync("test");
  }

  @Test
  void read() throws InterruptedException {
    KafkaClientTemplate.readOnlyClient("localhost:9092", "input-topic", "test-group-id")
        .concurrency(2)
        .iTaskHandler((event) -> System.out.println("Event Received = " + event))
        .create();
    Thread.sleep(5000);
  }

  @Test
  void readAndSend() {}
}
