package com.ni30.dlock.task;

import com.ni30.dlock.Constants;
import com.ni30.dlock.DLockCallback;
import com.ni30.dlock.SenderCallback;
import com.ni30.dlock.TaskLooperService;
import com.ni30.dlock.node.ClusterNode;
import com.ni30.dlock.node.ClusterNodeManager;
import com.ni30.dlock.node.LockRequestBucket;

/**
 * @author nitish.aryan
 */
public class CommandReceiverTask extends LoopTask {
	private final ClusterNodeManager clusterNodeManager;
	private final TaskLooperService taskLooperService;
	private ClusterNode senderNode;
	private final String[] commandArgs;
	
	public CommandReceiverTask(ClusterNodeManager clusterNodeManager, TaskLooperService taskLooperService, ClusterNode senderNode, String[] commandArgs) {
		this.clusterNodeManager = clusterNodeManager;
		this.taskLooperService = taskLooperService;
		this.senderNode = senderNode;
		this.commandArgs = commandArgs;
	}
	
	@Override
	public void execute() throws Exception {
		final String commandName = this.commandArgs[0];
		switch(commandName) {
			case Constants.TRY_LOCK_COMMAND_KEY:
				if(isReceivedByLeaderNode()) {
					tryLockCommand();
				}
				break;
			case Constants.CANCEL_LOCK_COMMAND_KEY:
				if(isReceivedByLeaderNode()) {
					cancelLockCommand();
				}
				break;
			case Constants.UNLOCK_COMMAND_KEY:
				if(isReceivedByLeaderNode()) {
					unlockCommand();
				}
				break;
			case Constants.LOCK_GRANTED_COMMAND_KEY:
				lockGrantedCommand();
				break;
			case Constants.NEW_LEADER_COMMAND_KEY:
				newLeaderCommand();
				break;
			case Constants.ELECT_LEADER_COMMAND_KEY:
				electLeaderCommand();
				break;
		}
	}
	
	private void tryLockCommand() {
		// command - [command-key, command-id, requested-by-node-name, lock-key, wait-time, lease-time]
		LockRequestBucket bucket = this.clusterNodeManager.getLockRequestBucket(commandArgs[3]);
		bucket.addNewRequest(commandArgs[2], commandArgs[1], Long.parseLong(commandArgs[4]), Long.parseLong(commandArgs[5]));
		if(!bucket.isLooping()) {
			synchronized(bucket) {
				if(!bucket.isLooping()) {
					bucket.loopIn();
					LockRequestProcessorTask task = new LockRequestProcessorTask(clusterNodeManager, taskLooperService, bucket);
					taskLooperService.addToNext(task);
				}
			}
		}
	}
	
	private void cancelLockCommand() {
		// command - [command-key, command-id, requested-by-node-name, lock-key, lock-request-command-id]
		LockRequestBucket bucket = this.clusterNodeManager.getLockRequestBucket(commandArgs[3]);
		LockRequestBucket.LockRequest req = bucket.getLockRequest(commandArgs[2], commandArgs[4]);
		if(req != null && req.isActive()) {
			req.deactivate();
		}
	}
	
	private void unlockCommand() {
		// command - [command-key, command-id, requested-by-node-name, lock-key, lock-command-id]
		LockRequestBucket bucket = this.clusterNodeManager.getLockRequestBucket(commandArgs[3]);
		LockRequestBucket.LockRequest currReq = bucket.getCurrentLockRequest();
		if(currReq != null && currReq.getNodeName().equals(commandArgs[2]) && currReq.getCommandId().equals(commandArgs[4])) {
			currReq.deactivate();
		}
	}
	
	private void lockGrantedCommand() {
		// command - [command-key, command-id, lock-command-id]
		DLockCallback callback = this.clusterNodeManager.getDLockCallback(commandArgs[2]);
		if(callback != null) {
			callback.onLockGrant();
		}
	}
	
	private void newLeaderCommand() {
		this.clusterNodeManager.onNewLeaderSelection(this.senderNode.getNodeName());
	}
	
	private void electLeaderCommand() {
		this.clusterNodeManager.electLeader();
	}
	
	private void redirectToRightLeader(SenderCallback callback) {
		CommandSenderTask task = new CommandSenderTask(clusterNodeManager.getLeaderNode(), this.commandArgs, callback);
		taskLooperService.addToNext(task);
	}
	
	private boolean isReceivedByLeaderNode() {
		ClusterNode leaderNode = this.clusterNodeManager.getLeaderNode();
		String nodeName = this.clusterNodeManager.getNodeName();
		
		if(nodeName.equals(leaderNode.getNodeName())) {
			return true;
		}
		
		SenderCallback callback = new SenderCallback() {
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
				redirectToRightLeader(this);
				e.printStackTrace();
			}
		};
		
		redirectToRightLeader(callback);
		
		return false;
	}
}
