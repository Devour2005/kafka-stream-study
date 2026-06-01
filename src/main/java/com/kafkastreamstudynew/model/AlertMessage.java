package com.kafkastreamstudy.model;

import java.time.LocalDateTime;
import lombok.Data;

@Data
public class AlertMessage {
	private String alertId;
	private String severity;
	private String message;
	private String source;
	private LocalDateTime timestamp;

	public AlertMessage() {}

	public AlertMessage(String severity, String message, String source) {
		this.alertId = java.util.UUID.randomUUID().toString();
		this.severity = severity;
		this.message = message;
		this.source = source;
		this.timestamp = LocalDateTime.now();
	}

	@Override
	public String toString() {
		return String.format("AlertMessage{alertId='%s', severity='%s', message='%s', source='%s'}",
				alertId, severity, message, source);
	}
}
