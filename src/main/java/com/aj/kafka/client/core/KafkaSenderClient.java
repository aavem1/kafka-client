package com.aj.kafka.client.core;

import com.aj.kafka.client.model.Message;
import com.aj.kafka.client.model.Result;

import java.util.List;
import java.util.concurrent.Future;

public interface KafkaSenderClient<K, V> extends KafkaClient {
  Result<V> send(Message<K, V> message);

  Future<Result<V>> sendAsync(Message<K, V> message);

  List<Future<Result<V>>> sendBatch(List<Message<K, V>> message);
}
