package com.aj.kafka.client.core;

import com.aj.kafka.client.model.Message;
import com.aj.kafka.client.model.Result;

import javax.naming.OperationNotSupportedException;
import java.util.List;
import java.util.concurrent.Future;

public interface KafkaSenderClient extends KafkaClient {
  Result send(Message message) ;

  Future<Result> sendAsync(Message message) ;

  List<Future<Result>> sendBatch(List<Message> message) ;
}
