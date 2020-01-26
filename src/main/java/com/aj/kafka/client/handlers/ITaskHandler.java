package com.aj.kafka.client.handlers;

public interface ITaskHandler<E> {

  void onMessage(E message);
}
