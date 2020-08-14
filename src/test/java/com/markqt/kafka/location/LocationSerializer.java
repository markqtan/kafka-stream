package com.markqt.kafka.location;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class LocationSerializer  implements Serializer<Location> {
	ObjectMapper mapper = new ObjectMapper();
	@Override
	public byte[] serialize(String topic, Location data) {
		try {
			return mapper.writeValueAsBytes(data);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
}
