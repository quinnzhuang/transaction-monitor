package com.mars.quinn.jdbc.monitor.event;

public interface TransactionListener {
	
	public void onBegin(TransactionEvent event);
	
	public void onCommit(TransactionEvent event);
	
	public void onRollback(TransactionEvent event);

}
