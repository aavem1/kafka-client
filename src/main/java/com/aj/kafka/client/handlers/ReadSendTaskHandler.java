package com.aj.kafka.client.handlers;

import com.aj.kafka.client.core.KafkaSenderClient;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.ExecutionException;

public abstract class ReadSendTaskHandler<E> implements ITaskHandler<E> {
  private KafkaSenderClient<String, E> kafkaSenderClient;

  public KafkaSenderClient<String, E> getKafkaSenderClient() {
    return kafkaSenderClient;
  }

  public void setKafkaSenderClient(KafkaSenderClient<String, E> kafkaSenderClient) {
    this.kafkaSenderClient = kafkaSenderClient;
  }

  @Transactional
  public void processEvent(E event) {

    onMessage(event);
    try {
      send(event);
    } catch (ExecutionException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public abstract void onMessage(E event);

  public abstract void send(E event) throws ExecutionException, InterruptedException;
}
