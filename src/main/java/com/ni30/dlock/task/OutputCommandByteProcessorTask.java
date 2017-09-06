package com.ni30.dlock.task;

import com.ni30.dlock.Common;
import com.ni30.dlock.Constants;
import com.ni30.dlock.TaskLooperService;
import com.ni30.dlock.node.ClusterNode;
import com.ni30.dlock.node.ClusterNodeManager;
import com.ni30.dlock.node.PipelineClosedException;

/**
 * @author nitish.aryan (iitd.nitish@gmail.com)
 */
public class OutputCommandByteProcessorTask extends LoopTask {
	private final ClusterNodeManager clusterNodeManager;
	private final TaskLooperService taskLooperService;
	private final ClusterNode clusterNode;
	
	public OutputCommandByteProcessorTask(ClusterNodeManager clusterNodeManager, TaskLooperService taskLooperService, ClusterNode clusterNode) {
		this.clusterNodeManager = clusterNodeManager;
		this.taskLooperService = taskLooperService;
		this.clusterNode = clusterNode;
	}

	@Override
	public void execute() throws Exception {
		boolean enqueueNextTime = true;
		try {
			byte[] out = this.clusterNode.getClusterNodePipeline().output();
			if (out == null) {
				return;
			}
			
			final String[] command = Common.getCommand(out);
			if(command == null) return;
			
			if(Constants.HEART_BEAT_COMMAND_KEY.equals(command[0])) {
				if(command.length != 3) {
					clusterNode.getClusterNodePipeline().getClusterNode().kill();
					throw new PipelineClosedException("invalid heartbeat command received from node - " + clusterNode.getNodeName());
				}
				
				long sentAt;
				try {
					sentAt = Long.parseLong(command[2]);
				} catch(NumberFormatException e) {
					clusterNode.kill();
					throw new PipelineClosedException("invalid heartbeat command received from node - " + clusterNode.getNodeName(), e);
				}
				
				this.clusterNode.heartbeat(sentAt);
				return;
			}
			
			this.taskLooperService.addToNext(new CommandReceiverTask(this.clusterNodeManager, this.taskLooperService, clusterNode, command));
		} catch (PipelineClosedException e) {
			enqueueNextTime = false;
		} finally {
			if (enqueueNextTime) {
				CurrentLoopTaskQueue.enqueue(this);
			}
		}
	}
}
