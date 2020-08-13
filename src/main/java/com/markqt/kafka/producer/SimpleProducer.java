package com.markqt.kafka.producer;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.markqt.kafka.model.PurchaseKey;
import com.markqt.kafka.partitioner.PurchaseKeyPartitioner;

/**
 * Example of a simple producer, not meant to run as a stand alone example.
 *
 * If desired to run this example change the ProducerRecord below to
 * use a real topic name and comment out line #33 below.
 */
public class SimpleProducer {



    public static void main(String[] args) {

    	Properties propsOverride = new Properties();
    	propsOverride.put("key.serializer", "com.markqt.kafka.ser.PurchaseKeySerializer");

        PurchaseKey key = new PurchaseKey("12334568", new Date());

        send("testTopic", propsOverride, key, "value-2");

    }

    public static<K, V> void send(String topic, K key, V value) {
    	send(topic, null, key, value);
    }
	public static<K, V> void send(String topic, Properties propsOverride, K key, V value) {
		Properties properties = config();
		if(propsOverride != null) {
			properties.putAll(propsOverride);
		}
		try(Producer<K, V> producer = new KafkaProducer<>(properties)) {
            ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, value);

            Callback callback = (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                }
            };

            Future<RecordMetadata> sendFuture = producer.send(record, callback);
        }
	}

	private static Properties config() {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", "1");
        properties.put("retries", "3");
        properties.put("compression.type", "snappy");
        //This line in for demonstration purposes
        properties.put("partitioner.class", PurchaseKeyPartitioner.class.getName());
        properties.put("key.serializer.class", "kafka.serializer.DefaultEncoder");
		return properties;
	}


}
