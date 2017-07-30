package com.ni30.dlock.task;

import com.ni30.dlock.Common;
import com.ni30.dlock.Constants;
import com.ni30.dlock.LockService;
import com.ni30.dlock.node.ClusterNodePipeline;
import com.ni30.dlock.node.PipelineClosedException;

/**
 * @author nitish.aryan (iitd.nitish@gmail.com)
 */
public class OutputCommandByteProcessorTask extends LoopTask {
	private final ClusterNodePipeline clusterNodePipeline;
	private final LockService lockService;

	public OutputCommandByteProcessorTask(ClusterNodePipeline clusterNodePipeline, LockService lockService) {
		this.clusterNodePipeline = clusterNodePipeline;
		this.lockService = lockService;
	}

	@Override
	public void execute() throws Exception {
		boolean enqueueNextTime = true;
		try {
			byte[] out = this.clusterNodePipeline.output();
			if (out == null) {
				return;
			}
			
			final String[] command = Common.getCommand(out);
			if(command == null) return;
			
			if(Constants.HEART_BEAT_COMMAND_KEY.equals(command[0])) {
				if(command.length != 3) {
					clusterNodePipeline.getClusterNode().kill();
					throw new PipelineClosedException("invalid heartbeat command received from node - " + clusterNodePipeline.getClusterNode().getNodeName());
				}
				
				long sentAt;
				try {
					sentAt = Long.parseLong(command[2]);
				} catch(NumberFormatException e) {
					clusterNodePipeline.getClusterNode().kill();
					throw new PipelineClosedException("invalid heartbeat command received from node - " + clusterNodePipeline.getClusterNode().getNodeName(), e);
				}
				
				this.clusterNodePipeline.getClusterNode().heartbeat(sentAt);
				return;
			}
			
			CurrentLoopTaskQueue.enqueue(new CommandReceiverTask(lockService, clusterNodePipeline.getClusterNode().getNodeName(), command));
		} catch (PipelineClosedException e) {
			enqueueNextTime = false;
		} finally {
			if (enqueueNextTime) {
				CurrentLoopTaskQueue.enqueue(this);
			}
		}
	}
}
