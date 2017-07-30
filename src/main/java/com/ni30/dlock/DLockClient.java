package com.ni30.dlock;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author nitish.aryan (iitd.nitish@gmail.com)
 */
public class DLockClient {
	private final DLockBootstrap bootstrap; 
	
	public DLockClient() throws Exception {
		this(new Properties());
	}
	
	public DLockClient(Properties properties) throws Exception {
		this.bootstrap = new DLockBootstrap(properties);
	}
	
	public DLock localLock(String key, long timeout, TimeUnit unit) throws DLockException {
		LockService lockservice = this.bootstrap.getLockService();
		return lockservice.localLock(key, timeout, unit);
	}
	
	public void open() throws Exception {
		this.bootstrap.start();
	}
	
	public void close() throws Exception {
		this.bootstrap.stop();
	}
}
