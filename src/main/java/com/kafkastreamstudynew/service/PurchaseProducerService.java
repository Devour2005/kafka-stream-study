package com.kafkastreamstudynew.service;

import com.kafkastreamstudynew.config.KafkaTopicsProperties;
import com.kafkastreamstudynew.model.Purchase;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class PurchaseProducerService {

	private final KafkaTemplate<String, Purchase> kafkaTemplate;
	private final KafkaTopicsProperties topicsProperties;

	public CompletableFuture<SendResult<String, Purchase>> publish(
			Purchase purchase) {

		Objects.requireNonNull(
				purchase,
				"Purchase must not be null"
		);

		Purchase purchaseToSend = purchase.toBuilder()
				.purchaseId(
						StringUtils.defaultIfBlank(
								purchase.getPurchaseId(),
								UUID.randomUUID().toString()
						)
				)
				.purchaseDate(
						Optional.ofNullable(purchase.getPurchaseDate())
								.orElse(LocalDateTime.now())
				)
				.build();

		return kafkaTemplate.send(
				topicsProperties.getTopics().getPurchaseInput(),
				purchaseToSend.getPurchaseId(),
				purchaseToSend
		);
	}
}