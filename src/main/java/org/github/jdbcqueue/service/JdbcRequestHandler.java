package org.github.jdbcqueue.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import org.github.jdbcqueue.model.HandleRequestException;
import org.github.jdbcqueue.model.Request;
import org.github.jdbcqueue.model.RequestException;
import org.github.jdbcqueue.model.Response;
import org.github.jdbcqueue.model.ResponseException;
import org.github.jdbcqueue.model.SaveRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides means to persist request (or requests using batching). Handle tasks
 * and store results in the same database in one transaction. Send response
 * back. Optionally delete handled request.
 * 
 * Allows to persist requests, handle, send responses.
 * 
 * Guarantees that one task will be handled by one node at a time.
 * 
 * Typical scenario will be a controller storing requests in a database.
 * Multiple instances of a service handling requests in multiple threads.
 * 
 * Usage: Create a subclass for {@link JdbcRequestHandler} Override
 * {@link JdbcRequestHandler#handle(Request, Connection)} and
 * {@link JdbcRequestHandler#respond(long, Response)}
 * 
 * Specify handling and
 * 
 * Optional: overrie SQL creating methods if needed
 * 
 * Read following article to implement skip locked for databases other than
 * PostgreSQL or Oracle
 * https://www.enterprisedb.com/blog/what-skip-locked-postgresql-95
 * 
 * 
 * 
 * 
 * @author yilativs
 *
 */
public abstract class JdbcRequestHandler {

	private final Logger logger;

	private final String table;
	private final DataSource dataSource;
	private final boolean deleteAfterResponseSent;
	private final int fetchRequestForHandlingLimit;
	private final int fetchResposeLimit;
	private final DatabaseType databaseType;

	public enum DatabaseType {
		POSTGRESQL(" FOR UPDATE SKIP LOCKED", "ON CONFLICT DO NOTHING", ""),
		ORACLE(" ", "", " FOR UPDATE SKIP LOCKED"),
		MY_SQL(" FOR UPDATE SKIP LOCKED", "", ""),
		MS_SQL(" FOR UPDATE READPAST", "", ""),
		DB2(" FOR UPDATE SKIP LOCKED DATA", "", "");

		private final String forUpdateSkipLocked;
		private String onConflict;
		private String forUpdateSkipLockedForId;

		private DatabaseType(String skipLockSql, String onConflict, String skipLockedForId) {
			this.forUpdateSkipLocked = skipLockSql;
			this.onConflict = onConflict;
			this.forUpdateSkipLockedForId = skipLockedForId;

		}

		public String getSkipLock() {
			return forUpdateSkipLocked;
		}

		public String getOnConflict() {
			return onConflict;
		}

		public String getForUpdateSkipLockedForId() {
			return forUpdateSkipLockedForId;
		}
	}

	protected JdbcRequestHandler(
			String table,
			DataSource dataSource,
			boolean deleteAfterResponseSent,
			int fetchRequestForHandlingLimit,
			int fetchResposeLimit,
			DatabaseType databaseType,
			Logger logger) {
		this.table = table;
		this.dataSource = dataSource;
		this.deleteAfterResponseSent = deleteAfterResponseSent;
		this.fetchRequestForHandlingLimit = fetchRequestForHandlingLimit;
		this.fetchResposeLimit = fetchResposeLimit;
		this.databaseType = databaseType;
		this.logger = logger;
	}

	protected JdbcRequestHandler(
			String table,
			DataSource dataSource,
			boolean deleteAfterResponseSent,
			int fetchTasksForHandlingLimit,
			int fetchTasksForNotificationLimit,
			DatabaseType databaseType) {
		this(table, dataSource, deleteAfterResponseSent, fetchTasksForHandlingLimit, fetchTasksForNotificationLimit,
				databaseType, LoggerFactory.getLogger(JdbcRequestHandler.class));
	}

	/**
	 * Finds and handles new tasks .
	 * 
	 * @throws SaveRequestException
	 * @throws HandleRequestException
	 * 
	 * @throws SQLException
	 * 
	 */
	public final void handle() throws SaveRequestException, HandleRequestException {
		try (Connection connection = dataSource.getConnection()) {
			logger.info("Handling of new tasks started.");
			connection.setAutoCommit(false);
			try (PreparedStatement statement = connection.prepareStatement(getSelectForUpdateNewRequests())) {
				try (ResultSet resultSet = statement.executeQuery()) {
					while (resultSet.next()) {
						Request request = new Request(resultSet.getLong(1), resultSet.getBytes(2));
						Optional<Long> optionalRequest = lockRequest(request.getId(), connection,
								getSelecteForUpdatedNotHandledIndividualRequest());
						if (optionalRequest.isPresent()) {
							Response response = handle(request, connection);
							save(request.getId(), response, connection);
						}
					}
				}
			}
			connection.commit();
			logger.info("Handling of new tasks completed successfully.");
		} catch (SQLException e) {
			logger.error("Handling of new tasks failed because of " + e.getMessage(), e);
			throw new SaveRequestException(e.getMessage(), e);
		}
	}

	protected Optional<Long> lockRequest(long requestId, Connection connection, String sql) throws SQLException {
		if (databaseType == DatabaseType.ORACLE) {
			// in case of Oracle we have to do individual locking.
			try (PreparedStatement statement = connection.prepareStatement(sql)) {
				statement.setLong(1, requestId);
				try (ResultSet restResultSet = statement.executeQuery()) {
					if (restResultSet.next()) {
						return Optional.of(requestId);
					} else {
						return Optional.empty();
					}
				}
			}
		} else {
			return Optional.of(requestId);
		}
	}

	/**
	 * Finds and handles new tasks fetchRequestForHandlingLimit.
	 * 
	 * @throws ResponseException
	 * 
	 * @throws SQLException
	 * 
	 */
	public final void respond() throws ResponseException {
		logger.info("Sending response.");
		try (Connection connection = dataSource.getConnection()) {
			connection.setAutoCommit(false);
			try (PreparedStatement statement = connection.prepareStatement(getSelecteForUpdatedHandledRequests())) {
				try (ResultSet resultSet = statement.executeQuery()) {
					while (resultSet.next()) {
						long requestId = resultSet.getLong(1);
						Optional<Long> optionalRequest = lockRequest(
								requestId, 
								connection,
								getSelecteForUpdatedNotNotifiedIndividualRequest()
								);
						if (optionalRequest.isPresent()) {
							respond(requestId, new Response(resultSet.getInt(2), resultSet.getBytes(3)));
							if (deleteAfterResponseSent) {
								delete(requestId, connection);
							} else {
								saveResponseSentTimestamp(requestId, connection);
							}
						}

					}
				}
			}
			connection.commit();
			logger.info("Sending response completed successfully.");
		} catch (SQLException e) {
			logger.error("Sending response failed because of " + e.getMessage(), e);
			throw new ResponseException(e.getMessage(), e);
		}
	}

	/**
	 * Deletes all existing requests
	 * 
	 * @return number of deleted requests
	 * @throws RequestException
	 */
	public final int deleteAll() throws RequestException {
		logger.info("deleting all requests");
		try (Connection connection = dataSource.getConnection()) {
			try (PreparedStatement statement = connection.prepareStatement(getDeleteAllSql())) {
				return statement.executeUpdate();
			}
		} catch (SQLException e) {
			throw new RequestException("failed to delete all requests because of " + e.getMessage(), e);
		}
	}

	/**
	 * Stores multiple new tasks in one transaction in a batch.
	 * 
	 * @param tasks
	 * @throws SQLException
	 * @throws SaveRequestException
	 */
	public final void add(List<Request> requests, boolean failIfTaskAlreadyExist) throws SaveRequestException {
		logger.info("Saving of new tasks started.");
		try (Connection connection = dataSource.getConnection()) {
			connection.setAutoCommit(false);
			try (PreparedStatement statement = connection.prepareStatement(getSaveNewRequestSql())) {
				for (Request request : requests) {
					statement.setLong(1, request.getId());
					statement.setBytes(2, request.getData());
					statement.addBatch();
				}
				int[] updateCounts = statement.executeBatch();
				if (failIfTaskAlreadyExist) {
					for (int i = 0; i < updateCounts.length; i++) {
						if (updateCounts[i] == 0) {
							throw new SaveRequestException("request already exist", requests.get(i));
						}
					}
				}
			}
			connection.commit();
			logger.info("Saving of new tasks completed successfully.");
		} catch (SQLException e) {
			logger.error("Saving  of new tasks failed because of " + e.getMessage(), e);
			throw new SaveRequestException(e.getMessage(), e);
		}
	}

	public List<Long> getNotHandledRequestIds() {
		return new SelectListQueryExecutor<Long>().extract(getSelectNotHandleRequestIdSql(),
				new RequestIdResultSetExtractor());
	}

	public List<Long> getNotNotifiedRequestIds() {
		return new SelectListQueryExecutor<Long>().extract(getSelectNotNotifiedRequestIdSql(),
				new RequestIdResultSetExtractor());
	}

	public List<Long> getNotifiedRequestIds() {
		return new SelectListQueryExecutor<Long>().extract(getSelectNotifiedRequestIdSql(),
				new RequestIdResultSetExtractor());
	}

	protected void save(long requestId, Response response, Connection connection) throws SaveRequestException {
		try (PreparedStatement preparedStatement = connection.prepareStatement(getSaveResponseSql())) {
			preparedStatement.setInt(1, response.getCode());
			preparedStatement.setBytes(2, response.getData());
			preparedStatement.setLong(3, requestId);
			preparedStatement.execute();
		} catch (SQLException e) {
			throw new SaveRequestException(e.getMessage(), e);
		}
	}

	protected void saveResponseSentTimestamp(long requestId, Connection connection) throws SQLException {
		execute(requestId, connection, getSetResponseNotificationTimestampSql());
	}

	protected void delete(long requestId, Connection connection) throws SQLException {
		execute(requestId, connection, getDeleteRequestSql());
	}

	protected void execute(long requestId, Connection connection, String sql) throws SQLException {
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setLong(1, requestId);
			statement.execute();
		}
	}

	@FunctionalInterface
	private interface ResultSetExtractor<T> {
		T extract(ResultSet restult) throws SQLException;
	}

	private static class RequestIdResultSetExtractor implements ResultSetExtractor<Long> {
		public Long extract(ResultSet resultSet) throws SQLException {
			return resultSet.getLong(1);
		}
	}

	private class SelectListQueryExecutor<T> {
		List<T> extract(String sql, ResultSetExtractor<T> resultSetExtractor, Object... args) {
			try (Connection connection = dataSource.getConnection()) {
				List<T> result = new ArrayList<>();
				try (PreparedStatement statement = connection.prepareStatement(sql)) {
					for (int i = 0; i < args.length; i++) {
						statement.setObject(i + 1, args[i]);
					}
					try (ResultSet resultSet = statement.executeQuery()) {
						while (resultSet.next()) {
							result.add(resultSetExtractor.extract(resultSet));
						}
					}
				}
				return result;
			} catch (SQLException e) {
				throw new IllegalStateException("Failed to load data beause of " + e.getMessage(), e);
			}
		}
	}

	/**
	 * Handles response.
	 * 
	 * @param request    request
	 * @param connection a connection to use during update
	 * @return result task
	 */
	protected abstract Response handle(Request request, Connection connection) throws HandleRequestException;

	/**
	 * Sends response back to client.
	 * 
	 * @param requestId
	 * @param response
	 * @throws ResponseException
	 */
	protected abstract void respond(long requestId, Response response) throws ResponseException;

	protected String getSelectNotHandleRequestIdSql() {
		return "SELECT request_id FROM " + table + " WHERE response_code IS NULL";
	}

	protected String getSelecteForUpdatedNotHandledIndividualRequest() {
		return "SELECT request_id FROM " + table + " WHERE response_code IS NULL and request_id=? "
				+ databaseType.forUpdateSkipLockedForId;
	}

	protected String getSelecteForUpdatedNotNotifiedIndividualRequest() {
		return "SELECT request_id FROM " + table
				+ " WHERE response_code IS NOT NULL and response_code IS NOT NULL and response_notification_timestamp IS NULL and request_id=? "
				+ databaseType.forUpdateSkipLockedForId;
	}

	protected String getSelectNotNotifiedRequestIdSql() {
		return "SELECT request_id FROM " + table
				+ " WHERE response_code IS NOT NULL and response_notification_timestamp IS NULL";
	}

	protected String getSelectNotifiedRequestIdSql() {
		return "SELECT request_id FROM " + table + " WHERE response_notification_timestamp IS NOT NULL";
	}

	protected String getSelectForUpdateNewRequests() {
		return "SELECT request_id, request FROM " + table + " WHERE response_code IS NULL FETCH FIRST  "
				+ fetchRequestForHandlingLimit + " ROW ONLY " + databaseType.forUpdateSkipLocked;
	}

	protected String getSelecteForUpdatedHandledRequests() {
		return "SELECT request_id, response_code, response FROM " + table
				+ " WHERE response_code IS NOT NULL and response_notification_timestamp IS NULL FETCH FIRST " + fetchResposeLimit + " ROW ONLY "
				+ databaseType.forUpdateSkipLocked;
	}

	protected String getSaveResponseSql() {
		return "UPDATE " + table
				+ " SET response_code = ?, response = ? WHERE request_id = ? AND response_code IS NULL";
	}

	protected String getSaveNewRequestSql() {
		return "INSERT INTO " + table + " (request_id, request) VALUES (?,?) " + databaseType.getOnConflict();
	}

	protected String getDeleteRequestSql() {
		return "DELETE FROM " + table + " WHERE request_id = ?";
	}

	protected String getDeleteAllSql() {
		return "DELETE FROM " + table;
	}

	protected String getSetResponseNotificationTimestampSql() {
		return "UPDATE " + table + " SET response_notification_timestamp=CURRENT_TIMESTAMP WHERE request_id = ?";
	}

}
