package com.kafkastreamstudynew.service;

import com.kafkastreamstudynew.config.KafkaTopicsProperties;
import com.kafkastreamstudynew.model.OrderMessage;
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
