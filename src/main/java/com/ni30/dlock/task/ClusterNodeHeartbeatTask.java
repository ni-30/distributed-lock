package com.ni30.dlock.task;

import com.ni30.dlock.Common;
import com.ni30.dlock.Constants;
import com.ni30.dlock.node.ClusterNodePipeline;
import com.ni30.dlock.node.PipelineClosedException;

/**
 * @author nitish.aryan
 */
public class ClusterNodeHeartbeatTask extends LoopTask {
	private final ClusterNodePipeline clusterNodePipeline;
	private long lastSentAt = -1;
	private long heartbeatInterval = 1000;
	
	public ClusterNodeHeartbeatTask(ClusterNodePipeline clusterNodePipeline) {
		this.clusterNodePipeline = clusterNodePipeline;
	}

	@Override
	public void execute() throws Exception {
		boolean enqueueNextTime = true;
		try {
			if(System.currentTimeMillis() - this.lastSentAt > this.heartbeatInterval) {
				byte[] byteCommand = Common.getByteCommand(Constants.HEART_BEAT_COMMAND_KEY, Common.uuid(), System.currentTimeMillis());
				this.clusterNodePipeline.input(byteCommand);
				this.lastSentAt = System.currentTimeMillis();
			}
		} catch(PipelineClosedException e) {
			enqueueNextTime = false;
		} finally {
			if(enqueueNextTime) {
				CurrentLoopTaskQueue.enqueue(this);
			}
		}
	}
}
