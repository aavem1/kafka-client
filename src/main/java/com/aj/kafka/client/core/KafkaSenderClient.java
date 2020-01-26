package com.aj.kafka.client.core;

import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.concurrent.ExecutionException;

public interface KafkaSenderClient<K, E> extends KafkaClient {
  default void send(E event) throws ExecutionException, InterruptedException {
    final ListenableFuture<SendResult<K, E>> listenableFuture = sendEvent(event);
    listenableFuture.get();
  }

  default void sendAsync(E event) {
    sendEvent(event);
  }

  ListenableFuture<SendResult<K, E>> sendEvent(E event);
}
