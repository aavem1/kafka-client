package com.aj.kafka.client.handlers;

import com.aj.kafka.client.core.KafkaSenderClient;

public abstract class ReadSendTaskHandler<E, NE> implements ITaskHandler<E> {
  private KafkaSenderClient<String, E> kafkaSenderClient;

  public void setKafkaSenderClient(KafkaSenderClient<String, E> kafkaSenderClient) {
    this.kafkaSenderClient = kafkaSenderClient;
  }

  public abstract void onMessage(E event);

  public abstract void send(NE event);
}
