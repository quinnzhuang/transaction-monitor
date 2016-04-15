package com.mars.quinn.jdbc.monitor;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.mars.quinn.jdbc.monitor.event.TransactionListener;

public class DataSourceMonitor implements DataSource {
	
	private final DataSource dataSource;
	
	private final List<TransactionListener> transactionListeners = new CopyOnWriteArrayList<>();
	
	private Queue<Connection> connectionQueue = new ConcurrentLinkedQueue<>();
	
	public DataSourceMonitor(DataSource dataSource) {
		this.dataSource = dataSource;
	}
	
	public void setTransactionListeners(TransactionListener... listeners) {
		if (listeners != null) {
			for (TransactionListener transactionListener : listeners) {
				transactionListeners.add(transactionListener);
			}
		}
	}
	
	/**
	 * @return the transactionListeners
	 */
	public Iterator<TransactionListener> getTransactionListeners() {
		return transactionListeners.iterator();
	}

	@Override
	public PrintWriter getLogWriter() throws SQLException {
		return dataSource.getLogWriter();
	}

	@Override
	public void setLogWriter(PrintWriter out) throws SQLException {
		dataSource.setLogWriter(out);
	}

	@Override
	public void setLoginTimeout(int seconds) throws SQLException {
		dataSource.setLoginTimeout(seconds);
	}

	@Override
	public int getLoginTimeout() throws SQLException {
		return dataSource.getLoginTimeout();
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return dataSource.getParentLogger();
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return dataSource.unwrap(iface);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return dataSource.isWrapperFor(iface);
	}

	@Override
	public Connection getConnection() throws SQLException {
		Connection connection = dataSource.getConnection();
		if (!(connection instanceof ConnectionMonitor)) {
			connection = new ConnectionMonitor(connection, this);
			connectionQueue.add(connection);
		}
		return connection;
	}

	@Override
	public Connection getConnection(String username, String password) throws SQLException {
		Connection connection = dataSource.getConnection(username, password);
		if (!(connection instanceof ConnectionMonitor)) {
			connection = new ConnectionMonitor(connection, this);
			connectionQueue.add(connection);
		}
		return connection;
	}
	
	public void removeConnection(Connection connection) {
		connectionQueue.remove(connection);
	}

}
