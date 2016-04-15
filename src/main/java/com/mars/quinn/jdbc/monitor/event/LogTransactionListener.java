package com.mars.quinn.jdbc.monitor.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogTransactionListener implements TransactionListener {

	private static final Logger logger = LoggerFactory.getLogger("jdbcmonitor");
	
	@Override
	public void onBegin(TransactionEvent event) {
	}

	@Override
	public void onCommit(TransactionEvent event) {
		for (String sql : event.getSqls()) {
			logger.info("Transaction: {}, sql:{}", event.transactionId(), sql);
		}
		logger.info("Transaction: {} commited, cost {}ms", event.transactionId(), event.getTransactionTime());
	}

	@Override
	public void onRollback(TransactionEvent event) {
		for (String sql : event.getSqls()) {
			logger.info("Transaction: {}, sql:{}", event.transactionId(), sql);
		}
		logger.info("Transaction: {} rollback, cost {}ms", event.transactionId(), event.getTransactionTime());
	}

}
