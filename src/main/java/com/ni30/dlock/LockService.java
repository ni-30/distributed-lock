package com.ni30.dlock;

import com.ni30.dlock.node.ClusterNodePipeline;
import com.ni30.dlock.task.CommandSenderTask;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class LockService {
	private final Map<String, ClusterNodePipeline> pipelines = new ConcurrentHashMap<>();
	private final Map<String, DLockImpl> locks = new ConcurrentHashMap<>();
	private final TaskLooperService taskLooperService;
	private final String localNodeName;
	
	public LockService(String localNodeName, TaskLooperService taskLooperService) {
		this.localNodeName = localNodeName;
		this.taskLooperService = taskLooperService;
	}
	
	public void add(ClusterNodePipeline clusterNodePipeline) {
		this.pipelines.put(clusterNodePipeline.getClusterNode().getNodeName(), clusterNodePipeline);
	}
	
	public DLock localLock(String key, long timeout, TimeUnit unit) throws DLockException {
		DLockImpl lock = this.locks.compute(key, (k,v) -> v == null ? new DLockImpl(key) : v);
		if(lock.lock(timeout, unit)) {
			return lock;
		}
		return null;
	}
	  
	public boolean grantRemoteLock(String key, String nodeName) {
		DLockImpl lock = this.locks.get(key);
		if(lock != null && lock.isLocked()) {
			CountDownLatch cdl = lock.getCurrentCountDownLatch();
			if(cdl != null && cdl.getCount() == 0) {
				return false; // lock is locally acquired
			}
		
			// two nodes trying to acquire same lock
			int p1 = this.priority(key, localNodeName);
			int p2 = this.priority(key, nodeName);
			
			return p1 > p2;
		}
		
		return true;
	}
	
	public void onLockGrant(String key, String lockId, String nodeName) {
		DLockImpl lock = this.locks.get(key);
		if(lock != null 
				&& lock.isLocked() 
				&& lockId.equals(lock.getCurrentLockId())) {
			
			CountDownLatch cdl = lock.getCurrentCountDownLatch();
			if(cdl != null) {
				cdl.countDown();
			}
		}
	}
	
	public void sendCommand(String nodeName, Object[] commmand) {
		final ClusterNodePipeline pipeline = pipelines.get(nodeName);
		if(pipeline != null) {
			if(pipeline.getClusterNode().isRunning()) {
				CommandSenderTask task = new CommandSenderTask(pipeline, commmand, null);
				taskLooperService.addToNext(task);
			} else {
				pipelines.remove(nodeName);
			}
		}
	}
	
	private int priority(String key, String nodeName) {
		return (key + nodeName).hashCode();
	}
	
	private class DLockImpl implements DLock {
		private final String key;
		private final ReentrantLock reentrantLock;
		private String currentLockId;
		private CountDownLatch currentCountDownLatch;
		
		public DLockImpl(String key) {
			this.key = key;
			this.reentrantLock = new ReentrantLock();
		}
		
		@Override
		public String getKey() {
			return this.key;
		}
		
		public String getCurrentLockId() {
			return this.currentLockId;
		}
		
		public CountDownLatch getCurrentCountDownLatch() {
			return this.currentCountDownLatch;
		}
		
		public boolean isLocked() {
			return this.reentrantLock.isLocked();
		}
		
		public boolean lock(long timeout, TimeUnit unit) throws DLockException {
			long endTime = System.currentTimeMillis() + unit.toMillis(timeout);
			
			boolean isAcquired;
			try {
				isAcquired = this.reentrantLock.tryLock(timeout, unit);
			} catch (InterruptedException e) {
				throw new DLockException(e);
			}
			
			try {
				if(isAcquired) {
					this.currentLockId = null;
					this.currentCountDownLatch = null;
				} else {
					return false;
				}
				
				final String lockId = Common.uuid();
				this.currentLockId = lockId;
				
				final List<ClusterNodePipeline> allowedPipelines = new LinkedList<>();
				
				Iterator<ClusterNodePipeline> iterator = pipelines.values().iterator();
				while(iterator.hasNext()) {
					final ClusterNodePipeline pipeline = iterator.next();
					if(pipeline.getClusterNode().isRunning()) {
						allowedPipelines.add(pipeline);
					} else {
						iterator.remove();
					}
				}
				
				if(!allowedPipelines.isEmpty()) {
					timeout = endTime - System.currentTimeMillis();
					final CountDownLatch countDownLatch = new CountDownLatch(allowedPipelines.size());
					this.currentCountDownLatch = countDownLatch;
					
					for(ClusterNodePipeline p : allowedPipelines) {
						CommandSenderTask task = new CommandSenderTask(p,
								new Object[] {Constants.LOCK_COMMAND_KEY,
									Common.uuid(),
									lockId,
									this.getKey(),
									timeout},
								new SenderCallbackImpl(lockId, countDownLatch));
						
						taskLooperService.addToNext(task);
					}
					
					timeout = endTime - System.currentTimeMillis();
					if(timeout > 0) {
						countDownLatch.wait(timeout);
					}
					
					if(countDownLatch.getCount() != 0) {
						this.release();
						return false;
					}
				}
				
				
				return true;
			} catch (Exception e) {
				this.release();
				throw new DLockException(e);
			}
		}

		@Override
		public void release() throws DLockException {
			if(this.reentrantLock.isLocked() && this.reentrantLock.isHeldByCurrentThread()) {
				this.reentrantLock.unlock();
			}
			
			if(!this.reentrantLock.isLocked() && !this.reentrantLock.hasQueuedThreads()) {
				locks.remove(getKey());
			}
		}
		
		public class SenderCallbackImpl implements SenderCallback {
			private final String lockId;
			private final CountDownLatch countDownLatch;
			private SenderCallbackImpl(String lockId, CountDownLatch countDownLatch) {
				this.lockId = lockId;
				this.countDownLatch = countDownLatch;
			}
			
			@Override
			public void preSending() {
				if(!lockId.equals(currentLockId)) {
					throw new RuntimeException("current lock id changed");
				}
			}

			@Override
			public void onSent() {
				// do nothing
			}

			@Override
			public void onSendingFailure(Throwable e) {
				countDownLatch.countDown();
			}
		}
	}
}
