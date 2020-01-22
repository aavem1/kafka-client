package com.aj.kafka.client.core;

public interface KafkaReaderClient extends KafkaClient {

  void start();

  void stopReader();

  void resumeReader();

  void pauseReader();
}
