package com.aj.kafka.client.handlers;

import org.springframework.kafka.listener.MessageListener;

public abstract class ITaskHandler implements MessageListener {
  @Override
  public abstract void onMessage(Object data);
}
