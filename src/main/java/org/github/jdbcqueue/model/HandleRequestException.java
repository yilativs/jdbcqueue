package org.github.jdbcqueue.model;

import java.sql.SQLException;

public class HandleRequestException extends RequestException {

	private static final long serialVersionUID = 1792910866164604453L;

	public HandleRequestException(String message, Throwable cause, Request request) {
		super(message, cause, request);
	}

	public HandleRequestException(String message, Request request) {
		super(message, request);
	}

	public HandleRequestException(String message, SQLException e) {
		this(message, e, null);
	}

}
