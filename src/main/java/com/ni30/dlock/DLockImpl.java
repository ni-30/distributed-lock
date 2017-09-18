package com.ni30.dlock;

import com.ni30.dlock.node.ClusterNodeManager;
import com.ni30.dlock.task.CommandSenderTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author nitish.aryan
 */

public class DLockImpl implements DLock {
	private ClusterNodeManager clusterNodeManager;
	private TaskLooperService taskLooperService;
	private CountDownLatch countDownLatch;
	private String commandId;
	private String key;
	private boolean isLocked;
	
	public DLockImpl(ClusterNodeManager clusterNodeManager, TaskLooperService taskLooperService, String key) {
		this.clusterNodeManager = clusterNodeManager;
		this.taskLooperService = taskLooperService;
		this.key = key;
	}

	@Override
	public boolean tryLock(long waitTime, long leaseTime, TimeUnit timeUnit) throws Exception {
		if(this.commandId != null) {
			throw new Exception("lock is already used once");
		}
		
		this.commandId = Common.uuid();
		Object[] command = new Object[] {
				Constants.TRY_LOCK_COMMAND_KEY,
				commandId,
				clusterNodeManager.getNodeName(),
				this.key,
				timeUnit.toMillis(waitTime),
				timeUnit.toMillis(leaseTime)
			};
		
		this.countDownLatch = new CountDownLatch(1);
		
		DLockCallback callback = new DLockCallbackImpl(System.currentTimeMillis(), timeUnit.toMillis(waitTime), timeUnit.toMillis(leaseTime));
		clusterNodeManager.putDLockCallback(commandId, callback);
		
		final Throwable[] error = {null}; 
		SenderCallback senderCallback = new SenderCallback() {
			@Override
			public void preSending() {
				// do nothing
			}

			@Override
			public void onSent() {
				// do nothing
			}
			
			@Override
			public void onSendingFailure(Throwable e) {
				error[0] = e;
				countDownLatch.countDown();
			}
		};
		
		CommandSenderTask task = new CommandSenderTask(clusterNodeManager.getLeaderNode(), command, senderCallback);
		taskLooperService.addToNext(task);
		
		try {
			countDownLatch.await(waitTime, timeUnit);
			boolean isCountZero = countDownLatch.getCount() == 0;
			if(!isCountZero) countDownLatch.countDown();
			if(error[0] != null) {
				throw new Exception(error[0]);
			}
			
			isLocked = isCountZero;
			return isLocked;
		} finally {
			clusterNodeManager.removeDLockCallback(commandId);
			if(!isLocked) {
				Object[] command2 = new Object[]{
						Constants.CANCEL_LOCK_COMMAND_KEY,
						Common.uuid(),
						clusterNodeManager.getNodeName(),
						key,
						commandId
					};
				
				CommandSenderTask task2 = new CommandSenderTask(clusterNodeManager.getLeaderNode(), command2, null);
				taskLooperService.addToNext(task2);
			}
		}
	}

	@Override
	public boolean isLocked() {
		return isLocked;
	}

	@Override
	public void unlock() {
		if(!isLocked) return;
		
		Object[] command = new Object[]{
				Constants.UNLOCK_COMMAND_KEY,
				Common.uuid(),
				clusterNodeManager.getNodeName(),
				this.key,
				commandId
			};
		try {
			CommandSenderTask task = new CommandSenderTask(clusterNodeManager.getLeaderNode(), command, null);
			taskLooperService.addToNext(task);
		} finally {
			clusterNodeManager.markUnlocked(key, commandId);
			isLocked = false;
		}
	}

	public class DLockCallbackImpl implements DLockCallback {
		private long startTime;
		private long waitTime;
		private long leaseTime;
		
		public DLockCallbackImpl(long startTime, long waitTime, long leaseTime) {
			this.startTime = startTime;
			this.waitTime = waitTime;
			this.leaseTime = leaseTime;
		}
		
		@Override
		public void onLockGrant() {
			if((System.currentTimeMillis() - startTime) >= waitTime || countDownLatch.getCount() == 0) {
				Object[] command = new Object[]{
						Constants.UNLOCK_COMMAND_KEY,
						Common.uuid(),
						clusterNodeManager.getNodeName(),
						key,
						commandId
					};
				
				CommandSenderTask task = new CommandSenderTask(clusterNodeManager.getLeaderNode(), command, null);
				taskLooperService.addToNext(task);
			} else {
				clusterNodeManager.addLockedKey(key, leaseTime, commandId);
				countDownLatch.countDown();
			}
		}
	} 
}
