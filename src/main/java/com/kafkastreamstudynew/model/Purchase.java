package com.kafkastreamstudynew.model;

import java.time.LocalDateTime;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class Purchase {

	String purchaseId;
	String firstName;
	String lastName;
	String customerId;
	String creditCardNumber;
	String itemPurchased;
	String department;
	String employeeId;
	int quantity;
	double price;
	LocalDateTime purchaseDate;
	String zipCode;
	String storeId;

	public static final class PurchaseBuilder {

		private static final String CC_NUMBER_REPLACEMENT = "xxxx-xxxx-xxxx-";

		public PurchaseBuilder maskCreditCard() {
			Objects.requireNonNull(this.creditCardNumber, "Credit Card can't be null");

			String[] parts = this.creditCardNumber.split("-");

			if (parts.length < 4) {
				this.creditCardNumber = "xxxx";
			} else {
				this.creditCardNumber =
						CC_NUMBER_REPLACEMENT + parts[3];
			}

			return this;
		}
	}
}
