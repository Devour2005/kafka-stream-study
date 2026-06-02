package com.kafkastreamstudynew.model;

import java.time.LocalDateTime;
import java.util.UUID;
import lombok.Data;

@Data
public class OrderMessage {

	private String orderId;
	private String productName;
	private int quantity;
	private double price;
	private String customerEmail;
	private LocalDateTime timestamp;

	public OrderMessage() {}

	public OrderMessage(String productName, int quantity, double price, String customerEmail) {
		this.orderId = UUID.randomUUID().toString();
		this.productName = productName;
		this.quantity = quantity;
		this.price = price;
		this.customerEmail = customerEmail;
		this.timestamp = LocalDateTime.now();
	}

	@Override
	public String toString() {
		return String.format("OrderMessage{orderId='%s', product='%s', quantity=%d, price=%.2f, email='%s'}",
				orderId, productName, quantity, price, customerEmail);
	}
}