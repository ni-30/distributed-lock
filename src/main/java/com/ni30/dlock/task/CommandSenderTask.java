package com.ni30.dlock.task;

import com.ni30.dlock.Common;
import com.ni30.dlock.SenderCallback;
import com.ni30.dlock.node.ClusterNodePipeline;

/**
 * @author nitish.aryan
 */
public class CommandSenderTask extends LoopTask {
	private final ClusterNodePipeline clusterNodePipeline;
	private final Object[] commandArgs;
	private final SenderCallback callback;
	
	public CommandSenderTask(ClusterNodePipeline clusterNodePipeline, Object[] commandArgs, SenderCallback callback) {
		this.clusterNodePipeline = clusterNodePipeline;
		this.commandArgs = commandArgs;
		this.callback = callback;
	}
	
	@Override
	public void execute() throws Exception {
		if(this.callback != null) this.callback.preSending();
		
		try {
			this.clusterNodePipeline.input(Common.getByteCommand(commandArgs));
		} catch(Exception e) {
			if(this.callback != null) this.callback.onSendingFailure(e);
			return;
		}
		
		if(this.callback != null) this.callback.onSent();
	}
}
