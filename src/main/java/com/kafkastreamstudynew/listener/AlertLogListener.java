package com.kafkastreamstudynew.listener;

import com.kafkastreamstudynew.model.AlertMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AlertLogListener {

	@KafkaListener(
			topics = "${app.kafka.topics.alerts-output}",
			groupId = "alert-logger",
			containerFactory = "alertKafkaListenerContainerFactory")
	public void onAlert(AlertMessage alert) {
		log.info("Alert from stream: {}", alert);
	}
}
