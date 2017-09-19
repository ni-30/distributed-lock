package com.ni30.dlock.task;

import com.ni30.dlock.Common;
import com.ni30.dlock.Constants;
import com.ni30.dlock.SenderCallback;
import com.ni30.dlock.TaskLooperService;
import com.ni30.dlock.node.ClusterNodeManager;
import com.ni30.dlock.node.LockRequestBucket;

/**
 * @author nitish.aryan
 */
public class LockRequestProcessorTask extends LoopTask {
	private final ClusterNodeManager clusterNodeManager;
	private final TaskLooperService taskLooperService;
	private final LockRequestBucket lockRequestBucket;
	
	public LockRequestProcessorTask(ClusterNodeManager clusterNodeManager, TaskLooperService taskLooperService, LockRequestBucket lockRequestBucket) {
		this.clusterNodeManager = clusterNodeManager;
		this.taskLooperService = taskLooperService;
		this.lockRequestBucket = lockRequestBucket; 
	}

	@Override
	public void execute() throws Exception {
		LockRequestBucket.LockRequest currReq = lockRequestBucket.getCurrentLockRequest();
		if(currReq != null && currReq.isActive()) {
			if((System.currentTimeMillis() - currReq.getLeaseTimestamp()) >= currReq.getLeaseTime()) {
				currReq.deactivate();
			} else {
				CurrentLoopTaskQueue.enqueue(this);
				return;
			}
		}
		
		while(lockRequestBucket.hasNext()) {
			final LockRequestBucket.LockRequest req = lockRequestBucket.next();
			if(!req.isActive()) {
				continue;
			} else if ((System.currentTimeMillis() - req.getTimestamp()) >= req.getWaitTime()) {
				req.deactivate();
				continue;
			}
			
			Object[] command = new Object[]{
					Constants.LOCK_GRANTED_COMMAND_KEY,
					Common.uuid(),
					req.getCommandId()
				};
			
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
					req.deactivate();
				}
			};
			
			req.setLeaseTimestamp(System.currentTimeMillis());
			CommandSenderTask task = new CommandSenderTask(clusterNodeManager.getNodeByName(req.getNodeName()), command, senderCallback);
			taskLooperService.addToNext(task);
			
			CurrentLoopTaskQueue.enqueue(this);
		}
	}
	
}
