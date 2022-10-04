package org.github.jdbcqueue.model;

import java.sql.SQLException;

public class RequestException extends Exception {

	private static final long serialVersionUID = 1L;

	private final Request request;

	public RequestException(String message, Throwable cause, Request request) {
		super(message, cause);
		this.request = request;
	}

	public RequestException(String message, Request request) {
		super(message);
		this.request = request;
	}

	public RequestException(String message, SQLException e) {
		this(message, e, null);
	}

	public Request getRequest() {
		return request;
	}

}
