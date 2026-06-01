package com.kafkastreamstudy.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "app.kafka")
public class KafkaTopicsProperties {

	private Topics topics = new Topics();
	private double orderValueThreshold = 100.0;

	@Data
	public static class Topics {
		private String ordersInput = "orders-input";
		private String alertsOutput = "alerts-output";
	}
}
