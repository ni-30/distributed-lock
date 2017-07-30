package com.ni30.dlock.task;

import com.ni30.dlock.node.ClusterNodeBufferPipeline;
import com.ni30.dlock.node.PipelineClosedException;
import java.nio.ByteBuffer;

/**
 * @author nitish.aryan (iitd.nitish@gmail.com)
 * 
 * Read buffer from input buffer pipe and write to socket channel (buffer size is fixed) 
 */
public class InputBufferWriterTask extends LoopTask {
	private final int inputBufferBucketSize;
	protected final ClusterNodeBufferPipeline clusterNodeBufferPipeline;
	
	public InputBufferWriterTask(ClusterNodeBufferPipeline clusterNodeBufferPipeline) {
		this.inputBufferBucketSize = clusterNodeBufferPipeline.getClusterNode().getMaxSocketInputBufferSize();
		this.clusterNodeBufferPipeline = clusterNodeBufferPipeline;
	}
	
	@Override
	public void execute() throws Exception {
		boolean enqueueNextTime = true;
		try {
			final ByteBuffer inputBufferBucket = ByteBuffer.allocate(this.inputBufferBucketSize);
			
			clusterNodeBufferPipeline.getClusterNode().inputBufferPipeSourceRead(inputBufferBucket);
			inputBufferBucket.flip();
			
			this.clusterNodeBufferPipeline.input(inputBufferBucket);
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
