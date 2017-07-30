package com.ni30.dlock.node;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author nitish.aryan (iitd.nitish@gmail.com)
 * 
 * read and write to socket channel
 */
public class ClusterNodeBufferPipeline extends PipelineIO<ByteBuffer> {
	protected final ClusterNode clusterNode;
	
	public ClusterNodeBufferPipeline(ClusterNode clusterNode) {
		this.clusterNode = clusterNode;
	}
	
	public ClusterNode getClusterNode() {
		return this.clusterNode;
	}
	
	@Override
	public ByteBuffer output() throws IOException {
		if(!this.clusterNode.isRunning()) {
			throw new PipelineClosedException("cluster node pipeline is closed");
		}
		
		final ByteBuffer outputBuffer = ByteBuffer.allocate(this.clusterNode.getMaxSocketOutputBufferSize());
		try {
			this.clusterNode.getSocketChannel().read(outputBuffer);
		} catch(IOException e) {
			this.clusterNode.kill();
			throw new PipelineClosedException("socket read IOException", e);
		}
		outputBuffer.flip();
		
		if(!outputBuffer.hasRemaining()) {
			return null;
		}
		
		return outputBuffer;
	}

	@Override
	public void input(ByteBuffer inputBuffer) throws IOException {
		if(!this.clusterNode.isRunning()) {
			throw new PipelineClosedException("cluster node pipeline is closed");
		}
		
		if(inputBuffer == null || !inputBuffer.hasRemaining()) return;
		
		try {
			this.clusterNode.getSocketChannel().write(inputBuffer);
		} catch(IOException e) {
			this.clusterNode.kill();
			throw new PipelineClosedException("socket write IOException", e);
		}
	}
}
