package com.ni30.dlock;

import java.util.concurrent.TimeUnit;

/**
 * @author nitish.aryan
 */
public interface DLock {
	boolean tryLock(long waitTime, long leaseTime, TimeUnit timeUnit) throws Exception;
	boolean isLocked();
	void unlock();
}