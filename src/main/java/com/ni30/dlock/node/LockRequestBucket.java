package com.ni30.dlock.node;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author nitish.aryan
 */
public class LockRequestBucket {
	private LinkedList<LockRequest> lockRequestQueue = new LinkedList<>();
	private Map<String, LockRequest> lockRequestKeyValue = new HashMap<>();
	private LockRequest currentLockRequest;
	private boolean isLooping;
	private final ReentrantReadWriteLock statusLock = new ReentrantReadWriteLock();
	
	public LockRequest getLockRequest(String nodeName, String commandId) {
		return lockRequestKeyValue.get(nodeName + ":" + commandId);
	}
	
	public void addNewRequest(String nodeName, String commandId, long waitTime, long leaseTime) {
		LockRequest req = new LockRequest(nodeName, commandId, waitTime, leaseTime);
		lockRequestKeyValue.put(req.getNodeName() + ":" + req.getCommandId(), req);
		lockRequestQueue.add(req);
	}
	
	public LockRequest getCurrentLockRequest() {
		return this.currentLockRequest;
	}
	
	public LockRequest next() {
		if(this.currentLockRequest != null) {
			lockRequestKeyValue.remove(currentLockRequest.getNodeName() + ":" + currentLockRequest.getCommandId());
		}
		
		this.currentLockRequest = lockRequestQueue.poll();
		return this.currentLockRequest;
	}
	
	public boolean hasNext() {
		return !lockRequestQueue.isEmpty();
	}
	
	public boolean isLooping() {
		ReentrantReadWriteLock.ReadLock lock = statusLock.readLock();
		lock.lock();
		try {
			return this.isLooping;
		} finally {
			lock.unlock();
		}
	}
	
	public boolean loopOut() {
		ReentrantReadWriteLock.WriteLock lock = statusLock.writeLock();
		lock.lock();
		try {
			if(this.hasNext()) {
				return false;
			}
			this.isLooping = false;
			return true;
		} finally {
			lock.unlock();
		}
	}
	
	public void loopIn() {
		ReentrantReadWriteLock.WriteLock lock = statusLock.writeLock();
		lock.lock();
		try {
			this.isLooping = true;
		} finally {
			lock.unlock();
		}
	}
	
	public class LockRequest {
		private long timestamp;
		private String nodeName;
		private String commandId;
		private long waitTime;
		private long leaseTime;
		private boolean isActive;
		private long leaseTimestamp;
		
		public LockRequest(String nodeName, String commandId, long waitTime, long leaseTime) {
			this.timestamp = System.currentTimeMillis();
			this.isActive = true;
			this.nodeName = nodeName;
			this.commandId = commandId;
			this.waitTime = waitTime;
			this.leaseTime = leaseTime;
		}
		
		public boolean isActive() {
			return this.isActive;
		}
		
		public void deactivate() {
			this.isActive = false;
		}
		
		public long getTimestamp() {
			return this.timestamp;
		}
		
		public void setLeaseTimestamp(long leaseTimestamp) {
			this.leaseTimestamp = leaseTimestamp;
		}
		
		public long getLeaseTimestamp() {
			return this.leaseTimestamp;
		}
		
		public long getWaitTime() {
			return this.waitTime;
		}
		
		public long getLeaseTime() {
			return this.leaseTime;
		}
		
		public String getCommandId() {
			return this.commandId;
		}
		
		public String getNodeName() {
			return this.nodeName;
		}
	}
}
