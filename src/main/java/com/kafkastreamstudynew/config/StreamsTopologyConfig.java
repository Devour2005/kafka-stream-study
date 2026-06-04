package com.kafkastreamstudynew.config;

import com.kafkastreamstudynew.model.AlertMessage;
import com.kafkastreamstudynew.model.OrderMessage;
import com.kafkastreamstudynew.model.Purchase;
import com.kafkastreamstudynew.model.RewardAccumulator;
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
	public KStream<String, OrderMessage> orderAlertTopology(
			StreamsBuilder builder,
			Serde<String> stringSerde,
			Serde<OrderMessage> orderMessageSerde,
			Serde<AlertMessage> alertMessageSerde) {

		String ordersTopic = topicsProperties.getTopics().getOrdersInput();
		String alertsTopic = topicsProperties.getTopics().getAlertsOutput();

		double threshold = topicsProperties.getOrderValueThreshold();

		KStream<String, OrderMessage> orders = builder.stream(
				ordersTopic,
				Consumed.with(stringSerde, orderMessageSerde));

		orders
				.peek((key, order) -> log.info("Received order: key={}, order={}", key, order))
				.filter((key, order) -> total(order) > threshold)
				.mapValues(order -> new AlertMessage(
						"HIGH_VALUE",
						"Order %s total %.2f exceeds threshold %.2f"
								.formatted(order.getOrderId(), total(order), threshold),
						"order-stream-processor"))
				.peek((key, alert) -> log.info("Emitting alert: key={}, alert={}", key, alert))
				.to(alertsTopic, Produced.with(stringSerde, alertMessageSerde));

		log.info("\n=== KAFKA STREAMS TOPOLOGY ===\n{}", builder.build().describe());

		return orders;
	}

	@Bean
	public KStream<String, Purchase> purchaseAlertTopology(
			StreamsBuilder builder,
			Serde<String> stringSerde,
			Serde<Purchase> purchaseSerde,
			Serde<AlertMessage> alertMessageSerde,
			Serde<RewardAccumulator> rewardAccumulatorSerde) {

		String purchaseTopic = topicsProperties.getTopics().getPurchaseInput();
		String alertsTopic = topicsProperties.getTopics().getAlertsOutput();
		String rewardTopic = topicsProperties.getTopics().getRewardAccumulator();
		double threshold = topicsProperties.getOrderValueThreshold();

		KStream<String, Purchase> purchasesStream = builder.stream(
				purchaseTopic,
				Consumed.with(stringSerde, purchaseSerde))
				.mapValues(p -> p.toBuilder().maskCreditCard().build());

		purchasesStream
				.peek((k, p) -> log.info("Masked purchase: {}", p))
				.to("purchase-masked-topic", Produced.with(stringSerde, purchaseSerde));

		purchasesStream
				.filter((key, purch) -> total(purch) > threshold)
				.mapValues(purch -> new AlertMessage(
						"HIGH_VALUE",
						"Purchase %s total %.2f exceeds threshold %.2f"
								.formatted(purch.getCustomerId(), total(purch), threshold),
						"purchase-stream-processor"))
				.peek((key, alert) -> log.info("Emitting alert: key={}, alert={}", key, alert))
				.to(alertsTopic, Produced.with(stringSerde, alertMessageSerde));

		purchasesStream
				.mapValues(r -> RewardAccumulator.builder().from(r).build())
				.to(rewardTopic, Produced.with(stringSerde, rewardAccumulatorSerde));

		return purchasesStream;
	}

	private static double total(Purchase purchase) {
		return purchase.getQuantity() * purchase.getPrice();
	}

	private static double total(OrderMessage orderMessage) {
		return orderMessage.getQuantity() * orderMessage.getPrice();
	}
}
