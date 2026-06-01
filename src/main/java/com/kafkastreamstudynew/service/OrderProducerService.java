package com.kafkastreamstudy.service;

import com.kafkastreamstudy.config.KafkaTopicsProperties;
import com.kafkastreamstudy.model.OrderMessage;
import java.util.HashMap;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class OrderProducerService {

	private final KafkaTemplate<String, OrderMessage> kafkaTemplate;
	private final KafkaTopicsProperties topicsProperties;

	public OrderMessage publish(OrderMessage order) {

		if (order.getOrderId() == null) {
			order = new OrderMessage(
					order.getProductName(),
					order.getQuantity(),
					order.getPrice(),
					order.getCustomerEmail());
		}
		if (order.getTimestamp() == null) {
			order.setTimestamp(java.time.LocalDateTime.now());
		}
		kafkaTemplate.send(topicsProperties.getTopics().getOrdersInput(), order.getOrderId(), order);
		return order;
	}
}
