package com.mars.quinn.jdbc.monitor.event;

import com.mars.quinn.jdbc.monitor.ConnectionMonitor.TransactionInfo;

public class TransactionEvent {
	
	private final TransactionInfo transaction;
	
	public TransactionEvent(TransactionInfo transaction) {
		this.transaction = transaction;
	}
	
	public long getTransactionTime() {
		return transaction.getCost();
	}
	
	public String[] getSqls() {
		return transaction.sqls();
	}
	
	public String transactionId() {
		return transaction.getId();
	}

}
