package com.aj.kafka.client.core;

public interface KafkaReaderClient<E> extends KafkaClient {

  void start();

  void stopReader();

  void resumeReader();

  void pauseReader();
}
