package org.github.jdbcqueue.service;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.github.jdbcqueue.model.Request;
import org.github.jdbcqueue.model.RequestException;
import org.github.jdbcqueue.model.Response;
import org.github.jdbcqueue.service.JdbcRequestHandler.DatabaseType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

@Testcontainers
public class JdbcRequestHandlerTest {

	private static final String POSTGRESQL_IMAGE_VERSION = "14";
	private static final String TEST_PASSWORD = "testPassword";
	private static final String TEST_DATABASE = "testDatabase";
	private static final String TEST_USER = "testUser";
	@Container
	private PostgreSQLContainer postgresqlContainer = new PostgreSQLContainer("postgres:" + POSTGRESQL_IMAGE_VERSION)
			.withDatabaseName(TEST_DATABASE)
			.withUsername(TEST_USER)
			.withPassword(TEST_PASSWORD);
	private DataSource dataSource;
	private List<Request> requests;
	private Map<Long, Response> requestIdToResponse;

	@BeforeEach
	void beforeTest() {
		dataSource = getDataSource(postgresqlContainer);
		requests = List.of(new Request(0, "request0".getBytes()), new Request(1, "request1".getBytes()));
		requestIdToResponse = new HashMap<>();
		for (Request request : requests) {
			requestIdToResponse.put(request.getId(), new Response((int) request.getId(), ("response" + request.getId()).getBytes()));
		}
	}

	@Test
	void addsNewRequests() throws RequestException {
		JdbcRequestHandler service = createHandler(dataSource, "test.test_task", requestIdToResponse, false, 1, 1);
		service.deleteAll();
		boolean failIfTaskAlreadyExist = true;
		service.add(requests, failIfTaskAlreadyExist);
		Assertions.assertEquals(2, service.getNotHandledRequestIds().size());
	}

	@Test()
	void failsToAddsDuplicatedRequests() throws RequestException {
		JdbcRequestHandler service = createHandler(dataSource, "test.test_task", requestIdToResponse, false, 1, 1);
		service.deleteAll();
		boolean failIfTaskAlreadyExist = true;
		service.add(requests, failIfTaskAlreadyExist);
		try {
			service.add(requests, failIfTaskAlreadyExist);
			Assertions.fail();
		} catch (RequestException e) {
			// expected
		}
	}

	@Test
	void handlesAndNotifiesOneByOne() throws RequestException {
		JdbcRequestHandler service = createHandler(dataSource, "test.test_task", requestIdToResponse, false, 1, 1);
		service.deleteAll();
		boolean failIfTaskAlreadyExist = true;
		service.add(requests, failIfTaskAlreadyExist);
		Assertions.assertEquals(2, service.getNotHandledRequestIds().size());
		Assertions.assertEquals(0, service.getNotNotifiedRequestIds().size());
		service.handle();
		Assertions.assertEquals(1, service.getNotNotifiedRequestIds().size());
		service.handle();
		Assertions.assertEquals(2, service.getNotNotifiedRequestIds().size());

		Assertions.assertEquals(2, requestIdToResponse.size());
		service.respond();
		Assertions.assertEquals(1, requestIdToResponse.size());
		Assertions.assertEquals(1, service.getNotNotifiedRequestIds().size());
		service.respond();
		Assertions.assertEquals(0, requestIdToResponse.size());
		Assertions.assertEquals(0, service.getNotNotifiedRequestIds().size());
	}

	@Test
	void handlesAndNotifiesWithBatches() throws RequestException {
		JdbcRequestHandler service = createHandler(dataSource, "test.test_task", requestIdToResponse, false, 2, 2);
		service.deleteAll();
		boolean failIfTaskAlreadyExist = true;
		service.add(requests, failIfTaskAlreadyExist);
		Assertions.assertEquals(2, service.getNotHandledRequestIds().size());
		Assertions.assertEquals(0, service.getNotNotifiedRequestIds().size());
		service.handle();
		Assertions.assertEquals(2, service.getNotNotifiedRequestIds().size());

		Assertions.assertEquals(2, requestIdToResponse.size());
		service.respond();
		Assertions.assertEquals(0, requestIdToResponse.size());
		Assertions.assertEquals(0, service.getNotNotifiedRequestIds().size());
		Assertions.assertEquals(2, service.getNotifiedRequestIds().size());
		Assertions.assertEquals(2, service.deleteAll());
	}

	@Test
	void deletesNotifiedTasksOnDemand() throws RequestException {
		boolean deleteAfterCompletionNotification = true;
		JdbcRequestHandler service = createHandler(dataSource, "test.test_task", requestIdToResponse, deleteAfterCompletionNotification, 2, 2);
		service.deleteAll();
		boolean failIfTaskAlreadyExist = true;
		service.add(requests, failIfTaskAlreadyExist);
		Assertions.assertEquals(2, service.getNotHandledRequestIds().size());
		Assertions.assertEquals(0, service.getNotNotifiedRequestIds().size());
		service.handle();
		Assertions.assertEquals(2, service.getNotNotifiedRequestIds().size());

		Assertions.assertEquals(2, requestIdToResponse.size());
		service.respond();
		Assertions.assertEquals(0, requestIdToResponse.size());
		Assertions.assertEquals(0, service.getNotNotifiedRequestIds().size());
		Assertions.assertEquals(0, service.deleteAll());
	}

	protected JdbcRequestHandler createHandler(
			DataSource dataSource, String tableName,
			Map<Long, Response> requestIdToResponse,
			boolean deleteAfterResponseSent,
			int fetchTasksForHandlingLimit,
			int fetchTasksForNotificationLimit) {
		return new JdbcRequestHandler(tableName, dataSource, deleteAfterResponseSent, fetchTasksForHandlingLimit, fetchTasksForNotificationLimit, DatabaseType.POSTGRESQL) {

			@Override
			protected void respond(long requestId, Response response) {
				if (response.equals(requestIdToResponse.get(requestId))) {
					requestIdToResponse.remove(requestId);
				}
			}

			@Override
			protected Response handle(Request request, Connection connection) {
				return requestIdToResponse.get(request.getId());
			}
		};
	}

	private DataSource getDataSource(PostgreSQLContainer<?> postgresSQLContainer) {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl("jdbc:tc:postgresql:" + POSTGRESQL_IMAGE_VERSION + ":///" + TEST_DATABASE + "?TC_DAEMON=true&TC_TMPFS=/testtmpfs:rw");
		config.setUsername(TEST_USER);
		config.setPassword(TEST_PASSWORD);
		DataSource dataSource = new HikariDataSource(config);
		Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").baselineOnMigrate(true).load().migrate();
		return dataSource;
	}

}
