package com.kafkastreamstudy.controller;

import com.kafkastreamstudy.config.KafkaTopicsProperties;
import com.kafkastreamstudy.model.OrderMessage;
import com.kafkastreamstudy.service.OrderProducerService;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class StudyController {

	private final OrderProducerService orderProducerService;
	private final KafkaTopicsProperties topicsProperties;
	private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

	@PostMapping("/orders")
	public ResponseEntity<OrderMessage> submitOrder(@RequestBody OrderMessage order) {
		OrderMessage published = orderProducerService.publish(order);
		return ResponseEntity.accepted().body(published);
	}

	@GetMapping("/topology")
	public ResponseEntity<Map<String, Object>> topology() {
		String kafkaStreamsTopology = streamsBuilderFactoryBean.getTopology().describe().toString();

		return ResponseEntity.ok(Map.of(
				"description", """
						orders-input  -->  filter(quantity * price > threshold)  -->  map(AlertMessage)  -->  alerts-output
						""".trim(),
				"ordersInputTopic", topicsProperties.getTopics().getOrdersInput(),
				"alertsOutputTopic", topicsProperties.getTopics().getAlertsOutput(),
				"orderValueThreshold", topicsProperties.getOrderValueThreshold(),
				"kafkaStreamsTopology", kafkaStreamsTopology));
	}
}
