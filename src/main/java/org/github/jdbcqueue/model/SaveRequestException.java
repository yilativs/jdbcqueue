package org.github.jdbcqueue.model;

import java.sql.SQLException;

public class SaveRequestException extends RequestException {

	private static final long serialVersionUID = 1792910866164604453L;

	public SaveRequestException(String message, Throwable cause, Request request) {
		super(message, cause, request);
	}

	public SaveRequestException(String message, Request request) {
		super(message, request);
	}

	public SaveRequestException(String message, SQLException e) {
		this(message, e, null);
	}

}
