package com.kafkastreamstudynew.config;

import com.kafkastreamstudynew.model.AlertMessage;
import com.kafkastreamstudynew.model.OrderMessage;
import com.kafkastreamstudynew.model.Purchase;
import com.kafkastreamstudynew.model.RewardAccumulator;
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
	public KStream<String, OrderMessage> orderAlertTopology(
			StreamsBuilder builder,
			Serde<String> stringSerde,
			Serde<OrderMessage> orderMessageSerde,
			Serde<AlertMessage> alertMessageSerde) {

		log.info("orderAlertTopology");

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

		topologyVisualization(ordersTopic, threshold, alertsTopic);

		return orders;
	}

/*	@Bean
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
					.filter((key, order) -> total(order) > threshold)
					.mapValues(order -> new AlertMessage(
							"HIGH_VALUE",
							"Order %s total %.2f exceeds threshold %.2f"
									.formatted(order.getOrderId(), total(order), threshold),
							"order-stream-processor"))
					.peek((key, alert) -> log.info("Emitting alert: key={}, alert={}", key, alert))
					.to(alertsTopic, Produced.with(stringSerde, alertMessageSerde));

			topologyVisualization(ordersTopic, threshold, alertsTopic);
		};
	}*/


	@Bean
	public KStream<String, Purchase> purchaseAlertTopology(
			StreamsBuilder builder,
			Serde<String> stringSerde,
			Serde<Purchase> purchaseSerde,
			Serde<AlertMessage> alertMessageSerde,
			Serde<RewardAccumulator> rewardAccumulatorSerde) {

		log.info("purchaseAlertTopology");

		String purchaseTopic = topicsProperties.getTopics().getPurchaseInput();
		String alertsTopic = topicsProperties.getTopics().getAlertsOutput();
		String rewardTopic = topicsProperties.getTopics().getRewardAccumulator();
		double threshold = topicsProperties.getOrderValueThreshold();

		KStream<String, Purchase> purchasesStream = builder.stream(
				purchaseTopic,
				Consumed.with(stringSerde, purchaseSerde));

		purchasesStream
				.filter((key, purch) -> total(purch) > threshold)
				.mapValues(purch -> new AlertMessage(
						"HIGH_VALUE",
						"Purchase %s total %.2f exceeds threshold %.2f"
								.formatted(purch.getCustomerId(), total(purch), threshold),
						"order-stream-processor"))
				.peek((key, alert) -> log.info("Emitting alert: key={}, alert={}", key, alert))
				.to(alertsTopic, Produced.with(stringSerde, alertMessageSerde));

		/*	purchasesStream
					.peek((key, purch) -> log.info("Received purchase: key={}, purchase={}", key, purch))
					.filter((key, purch) -> total(purch) > threshold)
					.mapValues(purch -> new AlertMessage(
							"HIGH_VALUE",
							"Purchase %s total %.2f exceeds threshold %.2f"
									.formatted(purch.getCustomerId(), total(purch), threshold),
							"order-stream-processor"))
					.peek((key, alert) -> log.info("Emitting alert: key={}, alert={}", key, alert))
					.to(alertsTopic, Produced.with(stringSerde, alertMessageSerde));*/

//			KStream<String, RewardAccumulator> rewards = purchasesStream.mapValues(r -> RewardAccumulator.builder().from(r).build());
//			rewards.peek((k, p) -> log.info("Reward: {}", p));
//			rewards.to(rewardTopic, Produced.with(stringSerde, rewardAccumulatorSerde));

		purchasesStream
				.mapValues(r -> RewardAccumulator.builder().from(r).build())
				.to(rewardTopic, Produced.with(stringSerde, rewardAccumulatorSerde));

		topologyVisualization(purchaseTopic, threshold, alertsTopic);
		return purchasesStream;
	}

	private static void topologyVisualization(String purchaseTopic, double threshold, String alertsTopic) {
		log.info("Topology built: {} -> filter(total > {}) -> map(Alert) -> {}",
				purchaseTopic, threshold, alertsTopic);
	}

	private static double total(Purchase purchase) {
		return purchase.getQuantity() * purchase.getPrice();
	}

	private static double total(OrderMessage orderMessage) {
		return orderMessage.getQuantity() * orderMessage.getPrice();
	}
}
