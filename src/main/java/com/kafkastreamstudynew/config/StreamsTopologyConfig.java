package com.kafkastreamstudy.config;

import com.kafkastreamstudy.model.AlertMessage;
import com.kafkastreamstudy.model.OrderMessage;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class StreamsTopologyConfig {

	private final KafkaTopicsProperties topicsProperties;

	@Bean
	public Consumer<StreamsBuilder> orderAlertTopology(
			Serde<String> stringSerde,
			Serde<OrderMessage> orderMessageSerde,
			Serde<AlertMessage> alertMessageSerde) {

		return builder -> {
			String ordersTopic = topicsProperties.getTopics().getOrdersInput();
			String alertsTopic = topicsProperties.getTopics().getAlertsOutput();
			double threshold = topicsProperties.getOrderValueThreshold();

			KStream<String, OrderMessage> orders = builder.stream(
					ordersTopic,
					Consumed.with(stringSerde, orderMessageSerde));

			orders
					.peek((key, order) -> log.info("Received order: key={}, order={}", key, order))
					.filter((key, order) -> orderTotal(order) > threshold)
					.mapValues(order -> new AlertMessage(
							"HIGH_VALUE",
							"Order %s total %.2f exceeds threshold %.2f"
									.formatted(order.getOrderId(), orderTotal(order), threshold),
							"order-stream-processor"))
					.peek((key, alert) -> log.info("Emitting alert: key={}, alert={}", key, alert))
					.to(alertsTopic, Produced.with(stringSerde, alertMessageSerde));

			log.info("Topology built: {} -> filter(total > {}) -> map(Alert) -> {}",
					ordersTopic, threshold, alertsTopic);
		};
	}

	private static double orderTotal(OrderMessage order) {
		return order.getQuantity() * order.getPrice();
	}
}
