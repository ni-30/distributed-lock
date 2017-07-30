package com.ni30.dlock.node;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author nitish.aryan
 */
public class ClusterNodePipeline extends PipelineIO<byte[]> {
	private final Object inputLock = new Object();
	private final Object outputLock = new Object();
	
	protected final ClusterNode clusterNode;
	private final ByteBuffer outputBufferBucket;
	
	public ClusterNodePipeline(ClusterNode clusterNode) {
		this.clusterNode = clusterNode;
		this.outputBufferBucket = ByteBuffer.allocate(this.clusterNode.getOutputBufferBucketSize());
	}
	
	public ClusterNode getClusterNode() {
		return this.clusterNode;
	}

	@Override
	public byte[] output() throws IOException {
		if(!this.clusterNode.isRunning()) {
			throw new PipelineClosedException("cluster node pipeline is closed.");
		}
		
		byte[] out = null;
		synchronized(outputLock) {
			if(!this.outputBufferBucket.hasRemaining()) {
				this.outputBufferBucket.clear();
			}
			
			this.clusterNode.outputBufferPipeSourceRead(this.outputBufferBucket);
			this.outputBufferBucket.flip();
			
			if(this.outputBufferBucket.hasRemaining()) {
				while(this.outputBufferBucket.hasRemaining()) {
					byte b = this.outputBufferBucket.get();
					if(b == 10) {
						int totalBufferSize = this.outputBufferBucket.limit();
						this.outputBufferBucket.flip();
						
						if(this.outputBufferBucket.hasRemaining()) {
							if(this.outputBufferBucket.remaining() > 1) {
								out = new byte[this.outputBufferBucket.remaining() - 1];
								this.outputBufferBucket.get(out);
							}
							
							this.outputBufferBucket.get();
						}
						
						this.outputBufferBucket.mark();
						this.outputBufferBucket.limit(totalBufferSize);
						this.outputBufferBucket.reset();
						
						if(!this.outputBufferBucket.hasRemaining()) {
							break;
						}
						
						byte[] remainingBytes = new byte[this.outputBufferBucket.remaining()];
						this.outputBufferBucket.get(remainingBytes);
						
						this.outputBufferBucket.clear();
						this.outputBufferBucket.put(remainingBytes);
						break;
					}
				}
			}
		}
		
		return out;
	}

	@Override
	public void input(byte[] in) throws IOException {
		if(!this.clusterNode.isRunning()) {
			throw new PipelineClosedException("cluster node pipeline is closed.");
		}
		
		synchronized (inputLock) {
			ByteBuffer inputBuffer = ByteBuffer.allocate(in.length + 1);
			inputBuffer.put(in);
			inputBuffer.put((byte) 10);
			inputBuffer.flip();
			
			this.clusterNode.inputBufferPipeSinkWrite(inputBuffer);
		}
	}
}
