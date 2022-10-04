package org.github.jdbcqueue.model;

import java.io.Serializable;
import java.util.Objects;

public class Request implements Serializable {

	private static final long serialVersionUID = 1L;
	private final long id;
	private final byte[] data;

	public Request(long id, byte[] data) {
		this.id = id;
		this.data = data;
	}

	public long getId() {
		return id;
	}

	public byte[] getData() {
		return data;
	}

	@Override
	public String toString() {
		return "Request [id=" + id + "]";
	}

	@Override
	public int hashCode() {
		return Objects.hash(id);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Request other = (Request) obj;
		return id == other.id;
	}

}
