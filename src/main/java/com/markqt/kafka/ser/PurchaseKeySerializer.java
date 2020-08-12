package com.markqt.kafka.ser;

import org.apache.kafka.common.serialization.Serializer;

import com.markqt.kafka.model.PurchaseKey;

public class PurchaseKeySerializer implements Serializer<PurchaseKey>{

	@Override
	public byte[] serialize(String topic, PurchaseKey data) {
		return (data.getCustomerId() + "-" + data.getTransactionDate().getTime()).getBytes();
	}

}
