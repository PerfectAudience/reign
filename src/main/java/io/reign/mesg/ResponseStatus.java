package io.reign.mesg;

import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * 
 * @author ypai
 * 
 */
@JsonSerialize(using = ResponseStatusSerializer.class)
public enum ResponseStatus {
	OK((byte) 0), ERROR_UNEXPECTED((byte) -1), ERROR_TIMED_OUT((byte) -2);

	private int code;

	ResponseStatus(int code) {
		this.code = code;
	}

	public int code() {
		return this.code;
	}
}
