package com.ni30.dlock.task;

import com.ni30.dlock.node.ClusterNodeBufferPipeline;
import com.ni30.dlock.node.PipelineClosedException;
import java.nio.ByteBuffer;

/**
 * @author nitish.aryan (iitd.nitish@gmail.com)
 * 
 * Read buffer from socket channel and write to output buffer pipe (buffer size is fixed)
 */
public class OutputBufferWriterTask extends LoopTask {
	protected final ClusterNodeBufferPipeline clusterNodeBufferPipeline;
	
	public OutputBufferWriterTask(ClusterNodeBufferPipeline clusterNodeBufferPipeline) {
		this.clusterNodeBufferPipeline = clusterNodeBufferPipeline;
	}
	
	@Override
	public void execute() throws Exception {
		boolean enqueueNextTime = true;
		try {
			ByteBuffer out = clusterNodeBufferPipeline.output();
			if(out != null) {
				clusterNodeBufferPipeline.getClusterNode().outputBufferPipeSinkWrite(out);
			}
		} catch(PipelineClosedException e) {
			enqueueNextTime = false;
			e.printStackTrace();
		} finally {
			if(enqueueNextTime) {
				CurrentLoopTaskQueue.enqueue(this);
			}
		}		
	}
}