package com.kafkastreamstudy.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkastreamstudy.model.AlertMessage;
import com.kafkastreamstudy.model.OrderMessage;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration
public class KafkaConfig {

	@Bean
	public ProducerFactory<String, OrderMessage> orderProducerFactory(
			KafkaProperties kafkaProperties,
			ObjectMapper objectMapper) {

		Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties(null));
		JsonSerializer<OrderMessage> valueSerializer = new JsonSerializer<>(objectMapper);
		valueSerializer.setAddTypeInfo(false);
		return new DefaultKafkaProducerFactory<>(
				props,
				new StringSerializer(),
				valueSerializer);
	}

	@Bean
	public KafkaTemplate<String, OrderMessage> orderKafkaTemplate(
			ProducerFactory<String, OrderMessage> orderProducerFactory) {
		return new KafkaTemplate<>(orderProducerFactory);
	}

	@Bean
	public ConsumerFactory<String, AlertMessage> alertConsumerFactory(
			KafkaProperties kafkaProperties,
			ObjectMapper objectMapper) {

		Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties(null));
		JsonDeserializer<AlertMessage> valueDeserializer =
				new JsonDeserializer<>(AlertMessage.class, objectMapper);
		valueDeserializer.addTrustedPackages("com.kafkastreamstudy.model");
		valueDeserializer.setUseTypeHeaders(false);
		return new DefaultKafkaConsumerFactory<>(
				props,
				new StringDeserializer(),
				valueDeserializer);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, AlertMessage> alertKafkaListenerContainerFactory(
			ConsumerFactory<String, AlertMessage> alertConsumerFactory) {

		ConcurrentKafkaListenerContainerFactory<String, AlertMessage> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(alertConsumerFactory);
		return factory;
	}
}
