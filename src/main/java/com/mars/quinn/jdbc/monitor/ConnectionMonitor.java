package com.mars.quinn.jdbc.monitor;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.mars.quinn.jdbc.monitor.event.TransactionEvent;

public class ConnectionMonitor implements Connection {

	private final Connection connection;

	private DataSourceMonitor dataSource;

	private List<TransactionInfo> current = new LinkedList<>();

	private List<TransactionInfo> history = new LinkedList<>();

	private Lock lock = new ReentrantLock();

	private static final Savepoint EMPTY_SAVEPOINT = new Savepoint() {

		@Override
		public int getSavepointId() throws SQLException {
			return 0;
		}

		@Override
		public String getSavepointName() throws SQLException {
			return null;
		}

	};

	public ConnectionMonitor(Connection connection, DataSourceMonitor dataSource) {
		this.connection = connection;
		this.dataSource = dataSource;
	}

	/**
	 * @param iface
	 * @return
	 * @throws SQLException
	 * @see java.sql.Wrapper#unwrap(java.lang.Class)
	 */
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return connection.unwrap(iface);
	}

	/**
	 * @param iface
	 * @return
	 * @throws SQLException
	 * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
	 */
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return connection.isWrapperFor(iface);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#createStatement()
	 */
	public Statement createStatement() throws SQLException {
		Statement statement = connection.createStatement();
		if (!(statement instanceof StatementMonitor)) {
			statement = new StatementMonitor(statement, maybeTransaction());
		}
		return statement;
	}

	/**
	 * @param sql
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#prepareStatement(java.lang.String)
	 */
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		PreparedStatement prepareStatement = connection.prepareStatement(sql);
		if (!(prepareStatement instanceof PreparedStatementMonitor)) {
			TransactionInfo transaction = maybeTransaction();
			transaction.addSql(sql);
			prepareStatement = new PreparedStatementMonitor(prepareStatement, transaction);
		}
		return prepareStatement;
	}

	/**
	 * @param sql
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#prepareCall(java.lang.String)
	 */
	public CallableStatement prepareCall(String sql) throws SQLException {
		CallableStatement callableStatement = connection.prepareCall(sql);
		if (!(callableStatement instanceof CallableStatementMonitor)) {
			TransactionInfo transaction = maybeTransaction();
			transaction.addSql(sql);
			callableStatement = new CallableStatementMonitor(callableStatement, transaction);
		}
		return callableStatement;
	}

	/**
	 * @param sql
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#nativeSQL(java.lang.String)
	 */
	public String nativeSQL(String sql) throws SQLException {
		return connection.nativeSQL(sql);
	}

	/**
	 * @param autoCommit
	 * @throws SQLException
	 * @see java.sql.Connection#setAutoCommit(boolean)
	 */
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		connection.setAutoCommit(autoCommit);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#getAutoCommit()
	 */
	public boolean getAutoCommit() throws SQLException {
		return connection.getAutoCommit();
	}

	/**
	 * @throws SQLException
	 * @see java.sql.Connection#commit()
	 */
	public void commit() throws SQLException {
		connection.commit();
		TransactionInfo transaction = transactionComplete(EMPTY_SAVEPOINT, TransactionState.COMMIT);
		if (transaction != null) {
			transaction.publishCommitEvent();
		}
	}

	/**
	 * @throws SQLException
	 * @see java.sql.Connection#rollback()
	 */
	public void rollback() throws SQLException {
		connection.rollback();
		TransactionInfo transaction = transactionComplete(EMPTY_SAVEPOINT, TransactionState.ROLLBACK);
		if (transaction != null)
			transaction.publishRollbackEvent();
	}

	/**
	 * @throws SQLException
	 * @see java.sql.Connection#close()
	 */
	public void close() throws SQLException {
		connection.close();
		dataSource.removeConnection(this);
		dataSource = null;
		current = null;
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#isClosed()
	 */
	public boolean isClosed() throws SQLException {
		return connection.isClosed();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#getMetaData()
	 */
	public DatabaseMetaData getMetaData() throws SQLException {
		return connection.getMetaData();
	}

	/**
	 * @param readOnly
	 * @throws SQLException
	 * @see java.sql.Connection#setReadOnly(boolean)
	 */
	public void setReadOnly(boolean readOnly) throws SQLException {
		connection.setReadOnly(readOnly);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#isReadOnly()
	 */
	public boolean isReadOnly() throws SQLException {
		return connection.isReadOnly();
	}

	/**
	 * @param catalog
	 * @throws SQLException
	 * @see java.sql.Connection#setCatalog(java.lang.String)
	 */
	public void setCatalog(String catalog) throws SQLException {
		connection.setCatalog(catalog);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#getCatalog()
	 */
	public String getCatalog() throws SQLException {
		return connection.getCatalog();
	}

	/**
	 * @param level
	 * @throws SQLException
	 * @see java.sql.Connection#setTransactionIsolation(int)
	 */
	public void setTransactionIsolation(int level) throws SQLException {
		connection.setTransactionIsolation(level);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#getTransactionIsolation()
	 */
	public int getTransactionIsolation() throws SQLException {
		return connection.getTransactionIsolation();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#getWarnings()
	 */
	public SQLWarning getWarnings() throws SQLException {
		return connection.getWarnings();
	}

	/**
	 * @throws SQLException
	 * @see java.sql.Connection#clearWarnings()
	 */
	public void clearWarnings() throws SQLException {
		connection.clearWarnings();
	}

	/**
	 * @param resultSetType
	 * @param resultSetConcurrency
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#createStatement(int, int)
	 */
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		Statement statement = connection.createStatement(resultSetType, resultSetConcurrency);
		if (statement instanceof StatementMonitor) {
			statement = new StatementMonitor(statement, maybeTransaction());
		}
		return statement;
	}

	/**
	 * @param sql
	 * @param resultSetType
	 * @param resultSetConcurrency
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int, int)
	 */
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		PreparedStatement prepareStatement = connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
		if (!(prepareStatement instanceof PreparedStatementMonitor)) {
			TransactionInfo transaction = maybeTransaction();
			transaction.addSql(sql);
			prepareStatement = new PreparedStatementMonitor(prepareStatement, transaction);
		}
		return prepareStatement;
	}

	/**
	 * @param sql
	 * @param resultSetType
	 * @param resultSetConcurrency
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#prepareCall(java.lang.String, int, int)
	 */
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		CallableStatement callableStatement = connection.prepareCall(sql, resultSetType, resultSetConcurrency);
		if (!(callableStatement instanceof CallableStatementMonitor)) {
			TransactionInfo transaction = maybeTransaction();
			transaction.addSql(sql);
			callableStatement = new CallableStatementMonitor(callableStatement, transaction);
		}
		return callableStatement;
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#getTypeMap()
	 */
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return connection.getTypeMap();
	}

	/**
	 * @param map
	 * @throws SQLException
	 * @see java.sql.Connection#setTypeMap(java.util.Map)
	 */
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		connection.setTypeMap(map);
	}

	/**
	 * @param holdability
	 * @throws SQLException
	 * @see java.sql.Connection#setHoldability(int)
	 */
	public void setHoldability(int holdability) throws SQLException {
		connection.setHoldability(holdability);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#getHoldability()
	 */
	public int getHoldability() throws SQLException {
		return connection.getHoldability();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#setSavepoint()
	 */
	public Savepoint setSavepoint() throws SQLException {
		Savepoint savepoint = connection.setSavepoint();
		newTransaction(savepoint);
		return savepoint;
	}

	/**
	 * @param name
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#setSavepoint(java.lang.String)
	 */
	public Savepoint setSavepoint(String name) throws SQLException {
		Savepoint savepoint = connection.setSavepoint(name);
		newTransaction(savepoint);
		return savepoint;
	}

	private TransactionInfo newTransaction(Savepoint savepoint) {
		lock.lock();
		TransactionInfo transaction = null;
		try {
			transaction = new TransactionInfo(savepoint);
			current.add(0, transaction);
		} finally {
			lock.unlock();
		}
		transaction.publishBeginEvent();
		return transaction;
	}

	/**
	 * @param savepoint
	 * @throws SQLException
	 * @see java.sql.Connection#rollback(java.sql.Savepoint)
	 */
	public void rollback(Savepoint savepoint) throws SQLException {
		connection.rollback(savepoint);
		TransactionInfo transaction = transactionComplete(savepoint, TransactionState.ROLLBACK);
		if (transaction != null)
			transaction.publishRollbackEvent();
	}

	private TransactionInfo transactionComplete(Savepoint savepoint, TransactionState state) {
		lock.lock();
		try {
			if (!current.isEmpty()) {
				addSqlsToMain(removeDisableTransaction(savepoint));
				TransactionInfo first = current.remove(0);
				if (first != null && !first.isComplete()) {
					first.complete(state);
					addToHistory(first);
				}
				return first;
			}
			return null;
		} finally {
			lock.unlock();
		}
	}

	private List<TransactionInfo> removeDisableTransaction(Savepoint savepoint) {
		List<TransactionInfo> disableList = new LinkedList<>();
		for (TransactionInfo transactionInfo : current) {
			if (transactionInfo.isSameTransaction(savepoint)) {
				break;
			}
			disableList.add(transactionInfo);
		}
		current.removeAll(disableList);
		return disableList;
	}
	
	private void addSqlsToMain(List<TransactionInfo> transactions) {
//		transactions.stream().
		for (int i = transactions.size() - 1; i > -1; i--) {
			TransactionInfo transaction = transactions.get(i);
			current.get(0).addAll(transaction.sqls());
		}
	}

	/**
	 * @param savepoint
	 * @throws SQLException
	 * @see java.sql.Connection#releaseSavepoint(java.sql.Savepoint)
	 */
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		connection.releaseSavepoint(savepoint);
		lock.lock();
		try {
			removeDisableTransaction(savepoint);
		} finally {
			lock.unlock();
		}
	}

	/**
	 * @param resultSetType
	 * @param resultSetConcurrency
	 * @param resultSetHoldability
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#createStatement(int, int, int)
	 */
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		Statement statement = connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
		if (statement instanceof StatementMonitor) {
			statement = new StatementMonitor(statement, maybeTransaction());
		}
		return statement;
	}

	/**
	 * @param sql
	 * @param resultSetType
	 * @param resultSetConcurrency
	 * @param resultSetHoldability
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int, int,
	 *      int)
	 */
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		PreparedStatement prepareStatement = connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
		if (!(prepareStatement instanceof PreparedStatementMonitor)) {
			TransactionInfo transaction = maybeTransaction();
			transaction.addSql(sql);
			prepareStatement = new PreparedStatementMonitor(prepareStatement, transaction);
		}
		return prepareStatement;
	}

	/**
	 * @param sql
	 * @param resultSetType
	 * @param resultSetConcurrency
	 * @param resultSetHoldability
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#prepareCall(java.lang.String, int, int, int)
	 */
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		CallableStatement callableStatement = connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
		if (!(callableStatement instanceof CallableStatementMonitor)) {
			TransactionInfo transaction = maybeTransaction();
			transaction.addSql(sql);
			callableStatement = new CallableStatementMonitor(callableStatement, transaction);
		}
		return callableStatement;
	}

	/**
	 * @param sql
	 * @param autoGeneratedKeys
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int)
	 */
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		PreparedStatement prepareStatement = connection.prepareStatement(sql, autoGeneratedKeys);
		if (!(prepareStatement instanceof PreparedStatementMonitor)) {
			TransactionInfo transaction = maybeTransaction();
			transaction.addSql(sql);
			prepareStatement = new PreparedStatementMonitor(prepareStatement, transaction);
		}
		return prepareStatement;
	}

	/**
	 * @param sql
	 * @param columnIndexes
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int[])
	 */
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		PreparedStatement prepareStatement = connection.prepareStatement(sql, columnIndexes);
		if (!(prepareStatement instanceof PreparedStatementMonitor)) {
			TransactionInfo transaction = maybeTransaction();
			transaction.addSql(sql);
			prepareStatement = new PreparedStatementMonitor(prepareStatement, transaction);
		}
		return prepareStatement;
	}

	/**
	 * @param sql
	 * @param columnNames
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#prepareStatement(java.lang.String,
	 *      java.lang.String[])
	 */
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		PreparedStatement prepareStatement = connection.prepareStatement(sql, columnNames);
		if (!(prepareStatement instanceof PreparedStatementMonitor)) {
			TransactionInfo transaction = maybeTransaction();
			transaction.addSql(sql);
			prepareStatement = new PreparedStatementMonitor(prepareStatement, transaction);
		}
		return prepareStatement;
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#createClob()
	 */
	public Clob createClob() throws SQLException {
		return connection.createClob();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#createBlob()
	 */
	public Blob createBlob() throws SQLException {
		return connection.createBlob();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#createNClob()
	 */
	public NClob createNClob() throws SQLException {
		return connection.createNClob();
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#createSQLXML()
	 */
	public SQLXML createSQLXML() throws SQLException {
		return connection.createSQLXML();
	}

	/**
	 * @param timeout
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#isValid(int)
	 */
	public boolean isValid(int timeout) throws SQLException {
		return connection.isValid(timeout);
	}

	/**
	 * @param name
	 * @param value
	 * @throws SQLClientInfoException
	 * @see java.sql.Connection#setClientInfo(java.lang.String,
	 *      java.lang.String)
	 */
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		connection.setClientInfo(name, value);
	}

	/**
	 * @param properties
	 * @throws SQLClientInfoException
	 * @see java.sql.Connection#setClientInfo(java.util.Properties)
	 */
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		connection.setClientInfo(properties);
	}

	/**
	 * @param name
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#getClientInfo(java.lang.String)
	 */
	public String getClientInfo(String name) throws SQLException {
		return connection.getClientInfo(name);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#getClientInfo()
	 */
	public Properties getClientInfo() throws SQLException {
		return connection.getClientInfo();
	}

	/**
	 * @param typeName
	 * @param elements
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#createArrayOf(java.lang.String,
	 *      java.lang.Object[])
	 */
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		return connection.createArrayOf(typeName, elements);
	}

	/**
	 * @param typeName
	 * @param attributes
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#createStruct(java.lang.String,
	 *      java.lang.Object[])
	 */
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		return connection.createStruct(typeName, attributes);
	}

	/**
	 * @param schema
	 * @throws SQLException
	 * @see java.sql.Connection#setSchema(java.lang.String)
	 */
	public void setSchema(String schema) throws SQLException {
		connection.setSchema(schema);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#getSchema()
	 */
	public String getSchema() throws SQLException {
		return connection.getSchema();
	}

	/**
	 * @param executor
	 * @throws SQLException
	 * @see java.sql.Connection#abort(java.util.concurrent.Executor)
	 */
	public void abort(Executor executor) throws SQLException {
		connection.abort(executor);
	}

	/**
	 * @param executor
	 * @param milliseconds
	 * @throws SQLException
	 * @see java.sql.Connection#setNetworkTimeout(java.util.concurrent.Executor,
	 *      int)
	 */
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		connection.setNetworkTimeout(executor, milliseconds);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @see java.sql.Connection#getNetworkTimeout()
	 */
	public int getNetworkTimeout() throws SQLException {
		return connection.getNetworkTimeout();
	}

	private void addToHistory(TransactionInfo transaction) {
		if (history.size() > 10) {
			history.remove(0);
		}
		history.add(transaction);
	}

	private TransactionInfo maybeTransaction() {
		lock.lock();
		TransactionInfo transaction = null;
		boolean isTransaction = false;
		try {
			if (current.isEmpty()) {
				transaction = new TransactionInfo();
				current.add(0, transaction);
				isTransaction = true;
			}
			transaction = current.get(0);
		} finally {
			lock.unlock();
		}
		if (isTransaction)
			transaction.publishBeginEvent();
		return transaction;
	}

	private enum TransactionState {
		COMMIT, ROLLBACK, RUNNING
	}

	public class TransactionInfo {

		private List<String> sqls = new CopyOnWriteArrayList<>();

		private final long beginTime = System.currentTimeMillis();

		private final String id = UUID.randomUUID().toString();
		
		private volatile TransactionState state;

		private long endTime;

		private final Savepoint savepoint;
		
		private final StackTraceElement[] startTrace = Thread.currentThread().getStackTrace();

		private StackTraceElement[] completeTrace;
		
		public TransactionInfo() {
			this(EMPTY_SAVEPOINT);
		}
		
		public String getId() {
			return id;
		}

		public TransactionInfo(Savepoint savepoint) {
			this.savepoint = savepoint;
			this.state = TransactionState.RUNNING;
		}
		
		public String[] sqls() {
			return sqls.toArray(new String[]{});
		}

		public void addSql(String sql) {
			sqls.add(sql);
		}
		
		void addAll(String... sqls) {
			this.sqls.addAll(Arrays.asList(sqls));
		}
		
		public void removeSqls(String... sqls) {
			this.sqls.removeAll(Arrays.asList(sqls));
		}

		public boolean isComplete() {
			return state == TransactionState.COMMIT || state == TransactionState.ROLLBACK;
		}

		public long getCost() {
			if (isComplete())
				return endTime - beginTime;
			return System.currentTimeMillis() - beginTime;
		}

		public void complete(TransactionState state) {
			if (!isComplete()) {
				endTime = System.currentTimeMillis();
				completeTrace = Thread.currentThread().getStackTrace();
				this.state = state;
			}
		}
		
		public StackTraceElement[] getStartTrace() {
			return startTrace;
		}
		
		public StackTraceElement[] getCompleteTrace() {
			if (isComplete())
				return completeTrace;
			throw new RuntimeException("There is no completeTrace when transaction has not bean completed.");
		}

		public boolean isSameTransaction(Savepoint savepoint) {
			return this.savepoint == savepoint;
		}
		
		public void publishBeginEvent() {
			if (dataSource != null) {
				dataSource.getListenerStream().forEach(t -> t.onBegin(new TransactionEvent(this)));
			}
		}
		
		public void publishCommitEvent() {
			if (dataSource != null) {
				dataSource.getListenerStream().forEach(t -> t.onCommit(new TransactionEvent(this)));
			}
		}
		
		public void publishRollbackEvent() {
			if (dataSource != null) {
				dataSource.getListenerStream().forEach(t -> t.onRollback(new TransactionEvent(this)));
			}
		}
	}

}
