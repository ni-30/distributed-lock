package com.ni30.dlock;

/**
 * @author nitish.aryan
 */
public class DLockException extends Exception {
	private static final long serialVersionUID = 1L;

	public DLockException(String string) {
		super(string);
	}
	
	public DLockException(Throwable e) {
		super(e);
	}

	public DLockException(String string, Throwable e) {
		super(string, e);
	}

}
