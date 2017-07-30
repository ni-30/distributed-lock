package com.ni30.dlock.task;

import com.ni30.dlock.Common;
import com.ni30.dlock.Constants;
import com.ni30.dlock.LockService;

/**
 * @author nitish.aryan
 */
public class CommandReceiverTask extends LoopTask {
	private final LockService lockService;
	private final String nodeName;
	private final String[] commandArgs;
	private int stage = 0;
	private int command;
	private long receivedAt;
	
	public CommandReceiverTask(LockService lockService, String nodeName, String[] commandArgs) {
		this.lockService = lockService;
		this.nodeName = nodeName;
		this.commandArgs = commandArgs;
		this.receivedAt = System.currentTimeMillis();
	}
	
	@Override
	public void execute() throws Exception {
		switch(this.stage) {
			case 0:
				this.stage = 1;
				switch(this.commandArgs[0]) {
					case Constants.LOCK_COMMAND_KEY:
						if(this.commandArgs.length < 5) {
							return;
						}
						this.command = 1;
						break;
					case Constants.LOCK_GRANTED_COMMAND_KEY:
						if(this.commandArgs.length < 3) {
							return;
						}
						this.command = 2;
						break;
					default:
						return;
				}
				
			case 1:
				switch(this.command) {
					case 1:
						long timeout = Long.parseLong(commandArgs[4]);
						if(System.currentTimeMillis() - this.receivedAt < timeout) {
							boolean isGranted = this.lockService.grantRemoteLock(this.commandArgs[3], this.nodeName);
							if(isGranted)  {
								Object[] replyCommand = new Object[]{
										Constants.LOCK_GRANTED_COMMAND_KEY,
										Common.uuid(),
										this.commandArgs[2],
										this.commandArgs[3]
									};
								this.lockService.sendCommand(this.nodeName, replyCommand);
							} else {
								CurrentLoopTaskQueue.enqueue(this);
							}
						}
						break;
					case 2:
						this.lockService.onLockGrant(this.commandArgs[3], this.commandArgs[2], this.nodeName);
						break;
					default:
						return;
				}
				
		}
	}
}
