package com.kafkastreamstudynew.config;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "app.kafka")
public class KafkaTopicsProperties {

	private Topics topics = new Topics();
	private TopicManagement topicManagement = new TopicManagement();
	private Map<String, String> topicsMap = new HashMap<>();
	private double orderValueThreshold = 100.0;

	@Data
	public static class Topics {

		private String ordersInput = "orders-input";
		private String purchaseInput = "purchase-input";
		private String alertsOutput = "alerts-output";
		private String rewardAccumulator = "reward-accumulator-output";
	}

	@Data
	public static class TopicManagement {
		//added for clear test results
		private boolean deleteOnShutdown = false;
		private int partitions = 1;
		private short replicationFactor = 1;
	}

	public Set<String> getAllTopicNames() {
		Set<String> topicNames = new LinkedHashSet<>();
		topicNames.add(topics.getOrdersInput());
		topicNames.add(topics.getPurchaseInput());
		topicNames.add(topics.getAlertsOutput());
		topicNames.add(topics.getRewardAccumulator());
		topicNames.addAll(topicsMap.values());
		topicNames.removeIf(topicName -> topicName == null || topicName.isBlank());
		return topicNames;
	}
}
