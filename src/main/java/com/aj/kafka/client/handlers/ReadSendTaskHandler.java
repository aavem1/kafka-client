package com.aj.kafka.client.handlers;

import org.springframework.kafka.listener.MessageListener;

public abstract class ReadSendTaskHandler implements MessageListener {
  @Override
  public abstract void onMessage(Object data);

  public final void send(Object data) {
    // code to send
    executeInTransaction(data);
  }

  public void executeInTransaction(Object data) {}
}
