package com.aj.kafka.client.handlers;

import com.aj.kafka.client.model.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;

public abstract class ITaskHandler<K, V> implements MessageListener<K, V> {
  @Override
  public void onMessage(ConsumerRecord<K, V> data) {
    // logic to extract the message and pass it to the onMessage method
    onMessage((Message<K, V>) null);
  }

  public abstract void onMessage(Message<K, V> message);
}
