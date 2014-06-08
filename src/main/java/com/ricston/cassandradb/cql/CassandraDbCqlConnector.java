/**
 *
 * Copyright (c) Ricston Ltd.  All rights reserved.  http://www.ricston.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.ricston.cassandradb.cql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.inject.Inject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mule.DefaultMuleEvent;
import org.mule.DefaultMuleMessage;
import org.mule.api.ConnectionException;
import org.mule.api.MuleContext;
import org.mule.api.MuleEvent;
import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.Connect;
import org.mule.api.annotations.ConnectStrategy;
import org.mule.api.annotations.ConnectionIdentifier;
import org.mule.api.annotations.Connector;
import org.mule.api.annotations.Disconnect;
import org.mule.api.annotations.Processor;
import org.mule.api.annotations.ValidateConnection;
import org.mule.api.annotations.display.Password;
import org.mule.api.annotations.param.ConnectionKey;
import org.mule.api.annotations.param.Default;
import org.mule.api.annotations.param.Optional;
import org.mule.api.expression.ExpressionManager;
import org.mule.util.StringUtils;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.ricston.cassandradb.cql.exception.InvalidTypeException;

/**
 * Cassandra CQL Connector
 * 
 * @author Alan Cassar, Ricston Ltd.
 */
@Connector(name = "cassandradbcql", schemaVersion = "1.0-SNAPSHOT", friendlyName = "Cassandra DB CQL")
public class CassandraDbCqlConnector {

	protected static final String EXPRESSION_FORMATTER = "#[%s]";

	/**
	 * Host name to connect to
	 */
	@Configurable
	@Default(value = "localhost")
	private String host;

	/**
	 * Port number to connect to
	 */
	@Configurable
	@Default(value = "9042")
	private Integer port;

	/**
	 * Keyspace to use
	 */
	@Optional
	@Configurable
	private String keyspace;

	private Cluster cluster;

	protected static Log logger = LogFactory
			.getLog(CassandraDbCqlConnector.class);

	private Map<PreparedStatementKey, PreparedStatement> preparedStatements = new ConcurrentHashMap<PreparedStatementKey, PreparedStatement>();

	/**
	 * Local pooling options
	 */
	@Optional
	@Configurable
	private CassandraDbCqlPoolingOptions localPoolingOptions;

	/**
	 * Remote pooling options
	 */
	@Optional
	@Configurable
	private CassandraDbCqlPoolingOptions remotePoolingOptions;

	/**
	 * Mule Expression Manager
	 */
	@Inject
	private ExpressionManager expressionManager;

	/**
	 * Mule Context
	 */
	@Inject
	private MuleContext context;

	/**
	 * Connect
	 * 
	 * @param username
	 *            A username
	 * @param password
	 *            A password
	 * @throws ConnectionException
	 */
	@Connect(strategy = ConnectStrategy.SINGLE_INSTANCE)
	public void connect(@ConnectionKey String username,
			@Password @Optional String password) throws ConnectionException {

		PoolingOptions poolingOptions = new PoolingOptions();

		Cluster.Builder builder = Cluster.builder().addContactPoint(host)
				.withPort(port).withPoolingOptions(poolingOptions);

		if (StringUtils.isNotBlank(password)) {
			AuthProvider authProvider = new PlainTextAuthProvider(username,
					password);
			builder = builder.withAuthProvider(authProvider);
		}

		// set local pooling options
		if (localPoolingOptions != null) {

			poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL,
					CassandraDbCqlUtils.defaultIfNull(localPoolingOptions
							.getCoreConnectionsPerHost(), poolingOptions
							.getCoreConnectionsPerHost(HostDistance.LOCAL)));

			poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL,
					CassandraDbCqlUtils.defaultIfNull(localPoolingOptions
							.getMaxConnectionsPerHost(), poolingOptions
							.getMaxConnectionsPerHost(HostDistance.LOCAL)));

			poolingOptions
					.setMinSimultaneousRequestsPerConnectionThreshold(
							HostDistance.LOCAL,
							CassandraDbCqlUtils.defaultIfNull(
									localPoolingOptions
											.getMinSimultaneousRequestsPerConnectionThreshold(),
									poolingOptions
											.getMinSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL)));

			poolingOptions
					.setMaxSimultaneousRequestsPerConnectionThreshold(
							HostDistance.LOCAL,
							CassandraDbCqlUtils.defaultIfNull(
									localPoolingOptions
											.getMaxSimultaneousRequestsPerConnectionThreshold(),
									poolingOptions
											.getMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL)));
		}

		// set remote pooling options
		if (remotePoolingOptions != null) {

			poolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE,
					CassandraDbCqlUtils.defaultIfNull(remotePoolingOptions
							.getCoreConnectionsPerHost(), poolingOptions
							.getCoreConnectionsPerHost(HostDistance.REMOTE)));

			poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE,
					CassandraDbCqlUtils.defaultIfNull(remotePoolingOptions
							.getMaxConnectionsPerHost(), poolingOptions
							.getMaxConnectionsPerHost(HostDistance.REMOTE)));

			poolingOptions
					.setMinSimultaneousRequestsPerConnectionThreshold(
							HostDistance.REMOTE,
							CassandraDbCqlUtils.defaultIfNull(
									remotePoolingOptions
											.getMinSimultaneousRequestsPerConnectionThreshold(),
									poolingOptions
											.getMinSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE)));

			poolingOptions
					.setMaxSimultaneousRequestsPerConnectionThreshold(
							HostDistance.REMOTE,
							CassandraDbCqlUtils.defaultIfNull(
									remotePoolingOptions
											.getMaxSimultaneousRequestsPerConnectionThreshold(),
									poolingOptions
											.getMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE)));
		}

		// build cluster and log information
		cluster = builder.build();
		CassandraDbCqlUtils.logClusterInformation(cluster);
	}

	/**
	 * Disconnect
	 */
	@Disconnect
	public void disconnect() {
		logger.info("Closing cluster " + cluster.getClusterName());
		cluster.close();
	}

	/**
	 * Are we connected
	 */
	@ValidateConnection
	public boolean isConnected() {
		return (cluster != null && !cluster.isClosed());
	}

	/**
	 * Connection identifier
	 */
	@ConnectionIdentifier
	public String connectionId() {
		return "na";
	}

	/**
	 * Performs an update statement on Cassandra, this might be INSERT, UPDATE
	 * ... Bulk mode is supported
	 * 
	 * {@sample.xml ../../../doc/CassandraDbCql-connector.xml.sample
	 * cassandradbcql:update}
	 * 
	 * @param cql
	 *            The CQL statement to execute
	 * @param params
	 *            The Mule parameters, can be expressions without the #[]
	 * @param bulkMode
	 *            Marks if we need to execute a batch, or a single statement
	 * @param event
	 *            The current Mule Event
	 * @throws Exception
	 *             if bulk mode is on, payload has to be collection
	 */
	@Processor
	@Inject
	public void update(String cql, @Optional List<String> params,
			@Default(value = "false") boolean bulkMode, MuleEvent event)
			throws InvalidTypeException {

		if (bulkMode == true) {
			Object payload = event.getMessage().getPayload();

			if (!(payload instanceof Collection)) {
				throw new InvalidTypeException(payload.getClass(),
						Collection.class);
			}
		}

		cassandraDoExecute(cql, params, bulkMode, event);
	}

	/**
	 * Performs a select statement on Cassandra
	 * 
	 * {@sample.xml ../../../doc/CassandraDbCql-connector.xml.sample
	 * cassandradbcql:select}
	 * 
	 * @param cql
	 *            The CQL statement to execute
	 * @param params
	 *            The Mule parameters, can be expressions without the #[]
	 * @param event
	 *            The current Mule Event
	 * @return List of Maps with results, each map represents a row, each entry
	 *         in the map represents a column
	 */
	@Processor
	@Inject
	public List<Map<String, Object>> select(String cql,
			@Optional List<String> params, MuleEvent event) {
		return cassandraDoExecute(cql, params, false, event);
	}

	/**
	 * Evaluates the parameter expressions and executes a cql statement
	 * 
	 * @param cql
	 *            The CQL statement to execute
	 * @param params
	 *            The Mule parameters, can be expressions without the #[]
	 * @param bulkMode
	 *            Marks if we need to execute a batch, or a single statement
	 * @param event
	 *            The current Mule Event
	 * @return List of Maps with results, each map represents a row, each entry
	 *         in the map represents a column
	 */
	public List<Map<String, Object>> cassandraDoExecute(String cql,
			List<String> params, boolean bulkMode, MuleEvent event) {

		// get mule context and expression manager
		// TODO: these should be automatically injected using @Inject
		MuleContext context = event.getMuleContext();
		ExpressionManager expressionManager = context.getExpressionManager();

		List<Object> evaluatedParameters = new ArrayList<Object>();
		int batchSize = 1;

		// if not in bulk mode, evaluate the expression for each parameter
		if (!bulkMode) {
			if (params != null) {
				for (String expression : params) {
					logger.debug("Evaluating: " + expression);
					expressionManager.validateExpression(String.format(
							EXPRESSION_FORMATTER, expression));
					evaluatedParameters.add(expressionManager.evaluate(
							String.format(EXPRESSION_FORMATTER, expression),
							event));
				}
			}
		}
		// if in bulk mode, evaluate the expression for each item in the list
		// payload
		else {
			@SuppressWarnings("unchecked")
			Collection<Object> collectionPayload = (Collection<Object>) event
					.getMessage().getPayload();
			batchSize = collectionPayload.size();

			for (Object payload : collectionPayload) {
				if (params != null) {
					for (String expression : params) {
						logger.debug("Evaluating: " + expression);
						expressionManager.validateExpression(String.format(
								EXPRESSION_FORMATTER, expression));
						evaluatedParameters
								.add(expressionManager.evaluate(String.format(
										EXPRESSION_FORMATTER, expression),
										new DefaultMuleEvent(
												new DefaultMuleMessage(payload,
														context), event)));
					}
				}
			}
		}

		// execute the statement using the evaluated parameters
		List<Row> result = cassandraExecuteStatement(cql, evaluatedParameters,
				batchSize);

		// convert result to list of maps and return
		return CassandraDbCqlUtils.toMaps(result);
	}

	/**
	 * Execute a Cassandra CQL statement with the given parameters
	 * 
	 * @param cql
	 *            The CQL statement
	 * @param parameters
	 *            The evaluated parameters
	 * @param batchSize
	 *            The size of the batch
	 * @return List of Cassandra Rows
	 */
	public List<Row> cassandraExecuteStatement(String cql,
			List<Object> parameters, int batchSize) {

		logger.debug("Executing statement: " + cql);

		// get session and prepared statement
		Session session = getSession();
		PreparedStatement statement = getPreparedStatement(cql, batchSize,
				session);

		// bind the parameters
		BoundStatement boundStatement = new BoundStatement(statement);
		boundStatement = boundStatement.bind(parameters.toArray());

		// execute statement
		ResultSet resultSet = session.execute(boundStatement);

		// read all results
		List<Row> rowList = resultSet.all();

		// close session and return
		session.close();
		return rowList;
	}

	/**
	 * Get Cassandra session
	 * 
	 * @return Cassandra Session
	 */
	protected Session getSession() {

		// if keyspace is not set, get a general session
		if (StringUtils.isBlank(keyspace)) {
			return cluster.connect();
		}

		// if keyspace is set, get a session for that keyspace
		return cluster.connect(keyspace);
	}

	/**
	 * Get a prepared statement from the cache if possible, if not, create it
	 * and put in cache
	 * 
	 * @param cql
	 * @param batchSize
	 * @param session
	 * @return
	 */
	protected PreparedStatement getPreparedStatement(String cql, int batchSize,
			Session session) {

		PreparedStatementKey key = new PreparedStatementKey(cql, batchSize);
		PreparedStatement statement = preparedStatements.get(key);

		if (statement == null) {
			String fullCql = makeStatment(cql, batchSize);
			statement = session.prepare(fullCql);
			preparedStatements.put(key, statement);
		}

		return statement;
	}

	/**
	 * Make string CQL depending on the batch size
	 * 
	 * @param cql
	 * @param batchSize
	 * @return
	 */
	protected String makeStatment(String cql, int batchSize) {
		if (batchSize == 1) {
			return cql;
		}

		// add ';' at the end of the statement if not present
		cql = StringUtils.trim(cql);
		if (!StringUtils.endsWith(cql, "; ")) {
			cql += ";";
		}

		// repeat the statement for the number of batches we need to execute
		StringBuilder batchCql = new StringBuilder();
		for (int i = 0; i < batchSize; i++) {
			batchCql.append(cql);
		}

		// surround with BEGIN BATCH & APPLLY BATCH
		return "BEGIN BATCH " + batchCql.toString() + "APPLY BATCH";
	}

	/**
	 * 
	 * @return
	 */
	public String getHost() {
		return host;
	}

	/**
	 * 
	 * @param host
	 */
	public void setHost(String host) {
		this.host = host;
	}

	/**
	 * 
	 * @return
	 */
	public Integer getPort() {
		return port;
	}

	/**
	 * 
	 * @param port
	 */
	public void setPort(Integer port) {
		this.port = port;
	}

	/**
	 * 
	 * @return
	 */
	public String getKeyspace() {
		return keyspace;
	}

	/**
	 * 
	 * @param keyspace
	 */
	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	/**
	 * 
	 * @return
	 */
	public Cluster getCluster() {
		return cluster;
	}

	/**
	 * 
	 * @param cluster
	 */
	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
	}

	/**
	 * 
	 * @param expressionManager
	 */
	public void setExpressionManager(ExpressionManager expressionManager) {
		this.expressionManager = expressionManager;
	}

	/**
	 * 
	 * @param context
	 */
	public void setContext(MuleContext context) {
		this.context = context;
	}

	/**
	 * 
	 * @return
	 */
	public CassandraDbCqlPoolingOptions getLocalPoolingOptions() {
		return localPoolingOptions;
	}

	/**
	 * 
	 * @param localPoolingOptions
	 */
	public void setLocalPoolingOptions(
			CassandraDbCqlPoolingOptions localPoolingOptions) {
		this.localPoolingOptions = localPoolingOptions;
	}

	/**
	 * 
	 * @return
	 */
	public CassandraDbCqlPoolingOptions getRemotePoolingOptions() {
		return remotePoolingOptions;
	}

	/**
	 * 
	 * @param remotePoolingOptions
	 */
	public void setRemotePoolingOptions(
			CassandraDbCqlPoolingOptions remotePoolingOptions) {
		this.remotePoolingOptions = remotePoolingOptions;
	}

}
