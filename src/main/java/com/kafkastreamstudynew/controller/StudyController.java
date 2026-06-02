package com.kafkastreamstudynew.controller;

import com.kafkastreamstudynew.config.KafkaTopicsProperties;
import com.kafkastreamstudynew.model.OrderMessage;
import com.kafkastreamstudynew.model.Purchase;
import com.kafkastreamstudynew.service.OrderProducerService;
import com.kafkastreamstudynew.service.PurchaseProducerService;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.support.SendResult;
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
	private final PurchaseProducerService purchaseProducerService;
	private final KafkaTopicsProperties topicsProperties;
	private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

	@PostMapping("/orders")
	public ResponseEntity<OrderMessage> submitOrder(@RequestBody OrderMessage order) {
		OrderMessage published = orderProducerService.publish(order);
		return ResponseEntity.accepted().body(published);
	}

	@PostMapping("/purchase")
	public ResponseEntity<Purchase> purchaseOrder(@RequestBody Purchase purchase) throws ExecutionException, InterruptedException {
		CompletableFuture<SendResult<String, Purchase>> publish = purchaseProducerService.publish(purchase);
		Purchase purchaseResult = publish.get().getProducerRecord().value();
		return ResponseEntity.accepted().body(purchaseResult);
	}

	/*@PostMapping("/purchase")
	public CompletableFuture<ResponseEntity<?>> purchaseOrder(
			@RequestBody Purchase purchase) {

		return purchaseProducerService.publish(purchase)
				.thenApply(result ->
						ResponseEntity.accepted().body(purchase))
				.exceptionally(ex ->
						ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
								.body(ex.getMessage()));
	}*/

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
