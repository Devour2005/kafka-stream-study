package com.kafkastreamstudynew.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkastreamstudynew.model.AlertMessage;
import com.kafkastreamstudynew.model.OrderMessage;
import com.kafkastreamstudynew.model.Purchase;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
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
		stripJsonSerializerConfig(props);
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
	public ProducerFactory<String, Purchase> purchaseProducerFactory(
			KafkaProperties kafkaProperties,
			ObjectMapper objectMapper) {

		Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties(null));
		stripJsonSerializerConfig(props);
		JsonSerializer<Purchase> valueSerializer = new JsonSerializer<>(objectMapper);
		valueSerializer.setAddTypeInfo(false);
		return new DefaultKafkaProducerFactory<>(
				props,
				new StringSerializer(),
				valueSerializer);
	}

	@Bean
	public KafkaTemplate<String, Purchase> purchaseKafkaTemplate(
			ProducerFactory<String, Purchase> orderProducerFactory) {
		return new KafkaTemplate<>(orderProducerFactory);
	}

	@Bean
	public ConsumerFactory<String, AlertMessage> alertConsumerFactory(
			KafkaProperties kafkaProperties,
			ObjectMapper objectMapper) {

		Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties(null));
		stripJsonDeserializerConfig(props);
		JsonDeserializer<AlertMessage> valueDeserializer =
				new JsonDeserializer<>(AlertMessage.class, objectMapper);
		valueDeserializer.addTrustedPackages("*");
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

	private static void stripJsonSerializerConfig(Map<String, Object> props) {
		props.remove(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
		props.remove(JsonSerializer.ADD_TYPE_INFO_HEADERS);
		props.remove(JsonSerializer.TYPE_MAPPINGS);
	}

	private static void stripJsonDeserializerConfig(Map<String, Object> props) {
		props.remove(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
		props.remove(JsonDeserializer.TRUSTED_PACKAGES);
		props.remove(JsonDeserializer.USE_TYPE_INFO_HEADERS);
		props.remove(JsonDeserializer.VALUE_DEFAULT_TYPE);
		props.remove(JsonDeserializer.KEY_DEFAULT_TYPE);
		props.remove(JsonDeserializer.TYPE_MAPPINGS);
	}
}
