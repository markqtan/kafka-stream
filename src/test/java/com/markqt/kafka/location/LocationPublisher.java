package com.markqt.kafka.location;

import java.util.Properties;

import com.markqt.kafka.producer.SimpleProducer;

public class LocationPublisher {
	public static void main(String[] args) {
		Properties propsOverride = new Properties();
    	propsOverride.put("value.serializer", "com.markqt.kafka.location.LocationSerializer");
    	propsOverride.put("value.serializer.class", "com.markqt.kafka.location.LocationSerializer");
		Location l = new Location();
		l.setLatitude(123.45);
		l.setLongitude(-456.78);
		l.setProfileId("abc123");
		SimpleProducer.send("locations", propsOverride, l.getProfileId(), l);
	}
}