package com.mars.quinn.jdbc.monitor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.mars.quinn.jdbc.monitor.ConnectionMonitor.TransactionInfo;

public class StatementMonitor implements Statement {
	
	private final Statement statement;
	
	private volatile TransactionInfo transaction;
	
	private Queue<String> batch = new ConcurrentLinkedQueue<>();
	
	public StatementMonitor(Statement statement, TransactionInfo transaction) {
		this.statement = statement;
		this.transaction = transaction;
	}

	/**
	 * @param iface
	 * @return
	 * @throws SQLException
	 * @see java.sql.Wrapper#unwrap(java.lang.Class)
	 */
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return statement.unwrap(iface);
	}

	/**
	 * @param sql
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#executeQuery(java.lang.String)
	 */
	public ResultSet executeQuery(String sql) throws SQLException {
		transaction.addSql(sql);
		return statement.executeQuery(sql);
	}

	/**
	 * @param iface
	 * @return
	 * @throws SQLException
	 * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
	 */
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return statement.isWrapperFor(iface);
	}

	/**
	 * @param sql
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#executeUpdate(java.lang.String)
	 */
	public int executeUpdate(String sql) throws SQLException {
		transaction.addSql(sql);
		return statement.executeUpdate(sql);
	}

	/**
	 * @throws SQLException
	 * @see java.sql.Statement#close()
	 */
	public void close() throws SQLException {
		statement.close();
		transaction = null;
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#getMaxFieldSize()
	 */
	public int getMaxFieldSize() throws SQLException {
		return statement.getMaxFieldSize();
	}

	/**
	 * @param max
	 * @throws SQLException
	 * @see java.sql.Statement#setMaxFieldSize(int)
	 */
	public void setMaxFieldSize(int max) throws SQLException {
		statement.setMaxFieldSize(max);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#getMaxRows()
	 */
	public int getMaxRows() throws SQLException {
		return statement.getMaxRows();
	}

	/**
	 * @param max
	 * @throws SQLException
	 * @see java.sql.Statement#setMaxRows(int)
	 */
	public void setMaxRows(int max) throws SQLException {
		statement.setMaxRows(max);
	}

	/**
	 * @param enable
	 * @throws SQLException
	 * @see java.sql.Statement#setEscapeProcessing(boolean)
	 */
	public void setEscapeProcessing(boolean enable) throws SQLException {
		statement.setEscapeProcessing(enable);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#getQueryTimeout()
	 */
	public int getQueryTimeout() throws SQLException {
		return statement.getQueryTimeout();
	}

	/**
	 * @param seconds
	 * @throws SQLException
	 * @see java.sql.Statement#setQueryTimeout(int)
	 */
	public void setQueryTimeout(int seconds) throws SQLException {
		statement.setQueryTimeout(seconds);
	}

	/**
	 * @throws SQLException
	 * @see java.sql.Statement#cancel()
	 */
	public void cancel() throws SQLException {
		statement.cancel();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#getWarnings()
	 */
	public SQLWarning getWarnings() throws SQLException {
		return statement.getWarnings();
	}

	/**
	 * @throws SQLException
	 * @see java.sql.Statement#clearWarnings()
	 */
	public void clearWarnings() throws SQLException {
		statement.clearWarnings();
	}

	/**
	 * @param name
	 * @throws SQLException
	 * @see java.sql.Statement#setCursorName(java.lang.String)
	 */
	public void setCursorName(String name) throws SQLException {
		statement.setCursorName(name);
	}

	/**
	 * @param sql
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#execute(java.lang.String)
	 */
	public boolean execute(String sql) throws SQLException {
		transaction.addSql(sql);
		return statement.execute(sql);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#getResultSet()
	 */
	public ResultSet getResultSet() throws SQLException {
		return statement.getResultSet();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#getUpdateCount()
	 */
	public int getUpdateCount() throws SQLException {
		return statement.getUpdateCount();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#getMoreResults()
	 */
	public boolean getMoreResults() throws SQLException {
		return statement.getMoreResults();
	}

	/**
	 * @param direction
	 * @throws SQLException
	 * @see java.sql.Statement#setFetchDirection(int)
	 */
	public void setFetchDirection(int direction) throws SQLException {
		statement.setFetchDirection(direction);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#getFetchDirection()
	 */
	public int getFetchDirection() throws SQLException {
		return statement.getFetchDirection();
	}

	/**
	 * @param rows
	 * @throws SQLException
	 * @see java.sql.Statement#setFetchSize(int)
	 */
	public void setFetchSize(int rows) throws SQLException {
		statement.setFetchSize(rows);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#getFetchSize()
	 */
	public int getFetchSize() throws SQLException {
		return statement.getFetchSize();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#getResultSetConcurrency()
	 */
	public int getResultSetConcurrency() throws SQLException {
		return statement.getResultSetConcurrency();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#getResultSetType()
	 */
	public int getResultSetType() throws SQLException {
		return statement.getResultSetType();
	}

	/**
	 * @param sql
	 * @throws SQLException
	 * @see java.sql.Statement#addBatch(java.lang.String)
	 */
	public void addBatch(String sql) throws SQLException {
		transaction.addSql(sql);
		batch.add(sql);
		statement.addBatch(sql);
	}

	/**
	 * @throws SQLException
	 * @see java.sql.Statement#clearBatch()
	 */
	public void clearBatch() throws SQLException {
		statement.clearBatch();
		transaction.removeSqls(batch.toArray(new String[]{}));
		batch.clear();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#executeBatch()
	 */
	public int[] executeBatch() throws SQLException {
		return statement.executeBatch();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#getConnection()
	 */
	public Connection getConnection() throws SQLException {
		return statement.getConnection();
	}

	/**
	 * @param current
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#getMoreResults(int)
	 */
	public boolean getMoreResults(int current) throws SQLException {
		return statement.getMoreResults(current);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#getGeneratedKeys()
	 */
	public ResultSet getGeneratedKeys() throws SQLException {
		return statement.getGeneratedKeys();
	}

	/**
	 * @param sql
	 * @param autoGeneratedKeys
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#executeUpdate(java.lang.String, int)
	 */
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		transaction.addSql(sql);
		return statement.executeUpdate(sql, autoGeneratedKeys);
	}

	/**
	 * @param sql
	 * @param columnIndexes
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#executeUpdate(java.lang.String, int[])
	 */
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		transaction.addSql(sql);
		return statement.executeUpdate(sql, columnIndexes);
	}

	/**
	 * @param sql
	 * @param columnNames
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#executeUpdate(java.lang.String, java.lang.String[])
	 */
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		transaction.addSql(sql);
		return statement.executeUpdate(sql, columnNames);
	}

	/**
	 * @param sql
	 * @param autoGeneratedKeys
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#execute(java.lang.String, int)
	 */
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		transaction.addSql(sql);
		return statement.execute(sql, autoGeneratedKeys);
	}

	/**
	 * @param sql
	 * @param columnIndexes
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#execute(java.lang.String, int[])
	 */
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		transaction.addSql(sql);
		return statement.execute(sql, columnIndexes);
	}

	/**
	 * @param sql
	 * @param columnNames
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#execute(java.lang.String, java.lang.String[])
	 */
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		transaction.addSql(sql);
		return statement.execute(sql, columnNames);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#getResultSetHoldability()
	 */
	public int getResultSetHoldability() throws SQLException {
		return statement.getResultSetHoldability();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#isClosed()
	 */
	public boolean isClosed() throws SQLException {
		return statement.isClosed();
	}

	/**
	 * @param poolable
	 * @throws SQLException
	 * @see java.sql.Statement#setPoolable(boolean)
	 */
	public void setPoolable(boolean poolable) throws SQLException {
		statement.setPoolable(poolable);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#isPoolable()
	 */
	public boolean isPoolable() throws SQLException {
		return statement.isPoolable();
	}

	/**
	 * @throws SQLException
	 * @see java.sql.Statement#closeOnCompletion()
	 */
	public void closeOnCompletion() throws SQLException {
		statement.closeOnCompletion();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#isCloseOnCompletion()
	 */
	public boolean isCloseOnCompletion() throws SQLException {
		return statement.isCloseOnCompletion();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#getLargeUpdateCount()
	 */
	public long getLargeUpdateCount() throws SQLException {
		return statement.getLargeUpdateCount();
	}

	/**
	 * @param max
	 * @throws SQLException
	 * @see java.sql.Statement#setLargeMaxRows(long)
	 */
	public void setLargeMaxRows(long max) throws SQLException {
		statement.setLargeMaxRows(max);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#getLargeMaxRows()
	 */
	public long getLargeMaxRows() throws SQLException {
		return statement.getLargeMaxRows();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#executeLargeBatch()
	 */
	public long[] executeLargeBatch() throws SQLException {
		return statement.executeLargeBatch();
	}

	/**
	 * @param sql
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#executeLargeUpdate(java.lang.String)
	 */
	public long executeLargeUpdate(String sql) throws SQLException {
		transaction.addSql(sql);
		return statement.executeLargeUpdate(sql);
	}

	/**
	 * @param sql
	 * @param autoGeneratedKeys
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#executeLargeUpdate(java.lang.String, int)
	 */
	public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		transaction.addSql(sql);
		return statement.executeLargeUpdate(sql, autoGeneratedKeys);
	}

	/**
	 * @param sql
	 * @param columnIndexes
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#executeLargeUpdate(java.lang.String, int[])
	 */
	public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
		transaction.addSql(sql);
		return statement.executeLargeUpdate(sql, columnIndexes);
	}

	/**
	 * @param sql
	 * @param columnNames
	 * @return
	 * @throws SQLException
	 * @see java.sql.Statement#executeLargeUpdate(java.lang.String, java.lang.String[])
	 */
	public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
		transaction.addSql(sql);
		return statement.executeLargeUpdate(sql, columnNames);
	}

}
