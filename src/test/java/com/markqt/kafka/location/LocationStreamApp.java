package com.markqt.kafka.location;

import java.util.Properties;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import com.markqt.kafka.util.serializer.JsonDeserializer;
import com.markqt.kafka.util.serializer.JsonSerializer;

public class LocationStreamApp {
	private static Properties getProperties() {
		Properties props = new Properties();
		props.put(StreamsConfig.CLIENT_ID_CONFIG, "Location-Streams-Client");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "Markqt-Location");
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Location-Streams--Streams-App");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
		props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
		return props;
	}

	public static void main(String[] args) throws Exception {
		StreamsConfig streamsConfig = new StreamsConfig(getProperties());
		Serde<String> stringSerde = Serdes.String();
		Serde<Location> locationSerde = new LocationSerde();
		StreamsBuilder streamsBuilder = new StreamsBuilder();
		KStream<String, Location> locationStream = streamsBuilder.stream("locations",
				Consumed.with(stringSerde, locationSerde));
		locationStream.print(Printed.<String, Location>toSysOut().withLabel("Locations"));

		Topology t = streamsBuilder.build();
		System.out.println(t.describe());
		KafkaStreams kafkaStreams = new KafkaStreams(t, streamsConfig);
		System.out.println("starting...");
		kafkaStreams.start();
		Thread.sleep(65000);
		System.out.println("shutdown");
		kafkaStreams.close();
	}
}

class Location {
	String profileId;
	double latitude;
	double longitude;

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
	public String getProfileId() {
		return profileId;
	}

	public void setProfileId(String profileId) {
		this.profileId = profileId;
	}

	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}
}

class LocationSerde extends WrapperSerde<Location> {
	public LocationSerde() {
		super(new JsonSerializer<>(), new JsonDeserializer<>(Location.class));
	}
}
