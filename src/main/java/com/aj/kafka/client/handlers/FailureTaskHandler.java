package com.aj.kafka.client.handlers;

import org.springframework.kafka.listener.AfterRollbackProcessor;

public interface FailureTaskHandler<K, V> extends AfterRollbackProcessor {}
