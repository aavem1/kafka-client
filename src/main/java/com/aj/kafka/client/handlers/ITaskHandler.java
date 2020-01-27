package com.aj.kafka.client.handlers;

public interface ITaskHandler<E> {

  default void processEvent(E event) {
    onMessage(event);
  }

  void onMessage(E event);
}
