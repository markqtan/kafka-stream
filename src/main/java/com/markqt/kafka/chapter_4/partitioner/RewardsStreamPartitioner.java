package com.markqt.kafka.chapter_4.partitioner;

import org.apache.kafka.streams.processor.StreamPartitioner;

import com.markqt.kafka.model.Purchase;


public class RewardsStreamPartitioner implements StreamPartitioner<String, Purchase> {

//    @Override
//    public Integer partition(String key, Purchase value, int numPartitions) {
//        return value.getCustomerId().hashCode() % numPartitions;
//    }

	@Override
	public Integer partition(String topic, String key, Purchase value, int numPartitions) {
	    return value.getCustomerId().hashCode() % numPartitions;
	 }
}
