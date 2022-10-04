package org.github.jdbcqueue.model;

import java.util.Arrays;
import java.util.Objects;

public class Response {

	private final int code;
	private final byte[] data;

	public Response(int code, byte[] data) {
		this.code = code;
		this.data = data;
	}

	public byte[] getData() {
		return data;
	}

	public int getCode() {
		return code;
	}

	@Override
	public String toString() {
		return "Response [code=" + code + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(data);
		result = prime * result + Objects.hash(code);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Response other = (Response) obj;
		return code == other.code && Arrays.equals(data, other.data);
	}
	
	

}
