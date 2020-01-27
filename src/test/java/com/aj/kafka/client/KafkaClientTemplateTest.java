package com.aj.kafka.client;

import com.aj.kafka.client.handlers.ReadSendTaskHandler;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

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
  void readAndSend() throws InterruptedException {
    final KafkaClientTemplate<String> kafkaClientTemplate =
        KafkaClientTemplate.transactionalClient(
                "localhost:9092", "input-topic-source", "input-topic-target", "test-group-id")
            .concurrency(2)
            .iTaskHandler(
                new ReadSendTaskHandler<String>() {

                  @Override
                  public void onMessage(String event) {
                    System.out.println("event = " + event);
                    try {
                      send(event);
                    } catch (ExecutionException e) {
                      e.printStackTrace();
                    } catch (InterruptedException e) {
                      e.printStackTrace();
                    }
                  }

                  @Override
                  public void send(String event) throws ExecutionException, InterruptedException {
                    // process the event
                    System.out.println("event = " + event);
                    // publish to target topic
                    this.getKafkaSenderClient().send(event);
                  }
                })
            .create();

    final KafkaClientTemplate<String> kafkaClient =
        KafkaClientTemplate.sendOnlyClient("localhost:9092", "input-topic-source")
            .beanName("bean-name")
            .create();
    kafkaClient.send("test-read-committed");
    Thread.sleep(10000);
  }
}
