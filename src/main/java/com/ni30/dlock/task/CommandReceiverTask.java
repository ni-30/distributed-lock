package com.ni30.dlock.task;

import com.ni30.dlock.Constants;
import com.ni30.dlock.SenderCallback;
import com.ni30.dlock.TaskLooperService;
import com.ni30.dlock.node.ClusterNode;
import com.ni30.dlock.node.ClusterNodeManager;

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
		case Constants.ELECT_LEADER_COMMAND_KEY:
			electLeaderCommand();
			break;
		}
	}
	
	private void tryLockCommand() {
		// TODO - will only be received by leader node
		// command - [command-key, command-id, lock-key, requested-by-node-name]
	}
	
	private void lockGrantedCommand() {
		// TODO
	}
	
	private void unlockCommand() {
		// TODO - will only be received by leader node
		// command - [command-key, command-id, lock-key, requested-by-node-name]
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
