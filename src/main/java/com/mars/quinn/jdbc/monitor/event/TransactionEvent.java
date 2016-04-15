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
	
	public StackTraceElement[] getStartTrace() {
		int length = transaction.getStartTrace().length;
		StackTraceElement[] trace = new StackTraceElement[length];
		System.arraycopy(transaction.getStartTrace(), 0, trace, 0, length);
		return trace;
	}
	
	public StackTraceElement[] getEndTrace() {
		int length = transaction.getCompleteTrace().length;
		StackTraceElement[] trace = new StackTraceElement[length];
		System.arraycopy(transaction.getCompleteTrace(), 0, trace, 0, length);
		return trace;
	}

}
