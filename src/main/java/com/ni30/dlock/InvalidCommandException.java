package com.ni30.dlock;

/**
 * @author nitish.aryan
 */
public class InvalidCommandException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public InvalidCommandException(String string) {
		super(string);
	}

	public InvalidCommandException(String string, Throwable e) {
		super(string, e);
	}
}