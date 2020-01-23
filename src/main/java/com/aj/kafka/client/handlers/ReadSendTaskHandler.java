package com.aj.kafka.client.handlers;

import com.aj.kafka.client.core.KafkaSenderClient;
import com.aj.kafka.client.model.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.transaction.annotation.Transactional;

public abstract class ReadSendTaskHandler<K, V> extends ITaskHandler<K, V> {
  private KafkaSenderClient<K, V> kafkaSenderClient;

  public void setKafkaSenderClient(KafkaSenderClient<K, V> kafkaSenderClient) {
    this.kafkaSenderClient = kafkaSenderClient;
  }

  @Override
  @Transactional
  public void onMessage(ConsumerRecord<K, V> data) {
    // logic to extract the message and pass it to the onMessage method
    onMessage((Message<K, V>) null);

    kafkaSenderClient.send(send(null));
  }

  public abstract void onMessage(Message<K, V> message);

  public abstract Message<K, V> send(Message<K, V> message);
}
