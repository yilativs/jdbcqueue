package org.github.jdbcqueue.model;

import java.sql.SQLException;

public class ResponseException extends RequestException {

	private static final long serialVersionUID = 1792910866164604453L;

	public ResponseException(String message, Throwable cause, Request request) {
		super(message, cause, request);
	}

	public ResponseException(String message, Request request) {
		super(message, request);
	}

	public ResponseException(String message, SQLException e) {
		this(message, e, null);
	}

}
