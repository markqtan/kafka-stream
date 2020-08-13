package com.markqt.kafka;

import com.markqt.kafka.producer.SimpleProducer;

public class WordCountApplicationPublisher {
	public static void main(String[] args) {
		SimpleProducer.send("TextLinesTopic", "", "Hi Mark");
	}
}
