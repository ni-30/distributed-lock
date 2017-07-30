package com.ni30.dlock;

/**
 * @author nitish.aryan
 */
public interface DLock {
	String getKey();
	void release() throws DLockException;
}