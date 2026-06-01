package com.kafkastreamstudy.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkastreamstudy.model.AlertMessage;
import com.kafkastreamstudy.model.OrderMessage;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
public class KafkaSerdeConfig {

	@Bean
	public Serde<String> stringSerde() {
		return Serdes.String();
	}

	@Bean
	public Serde<OrderMessage> orderMessageSerde(ObjectMapper objectMapper) {
		return new JsonSerde<>(OrderMessage.class, objectMapper);
	}

	@Bean
	public Serde<AlertMessage> alertMessageSerde(ObjectMapper objectMapper) {
		return new JsonSerde<>(AlertMessage.class, objectMapper);
	}
}
