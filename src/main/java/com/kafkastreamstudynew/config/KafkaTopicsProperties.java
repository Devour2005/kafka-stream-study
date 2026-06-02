package com.kafkastreamstudynew.config;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "app.kafka")
public class KafkaTopicsProperties {

	private Topics topics = new Topics();
	private Map<String, String> topicsMap = new HashMap<>();
	private double orderValueThreshold = 100.0;

	@Data
	public static class Topics {

		private String ordersInput = "orders-input";
		private String purchaseInput = "purchase-input";
		private String alertsOutput = "alerts-output";
		private String rewardAccumulator = "reward-accumulator-output";
	}

	public Set<String> getAllTopicNames() {
		return new HashSet<>(topicsMap.values());
	}
}
