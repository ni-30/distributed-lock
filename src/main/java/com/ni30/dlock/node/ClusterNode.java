package com.ni30.dlock.node;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.nio.channels.SocketChannel;
import java.util.UUID;

/**
 * @author nitish.aryan (iitd.nitish@gmail.com)
 */
public class ClusterNode {
	private String id;
	private String nodeName;
	private SocketChannel socketChannel;
	private boolean isKilled = false;
	private final Object killLock = new Object();
	
	private final int maxSocketOutputBufferSize = 1024;
	private final int maxSocketInputBufferSize = 1024;
	private final int outputBufferBucketSize = 1024;
	
	private final Pipe inputBufferPipe;
	private final Pipe outputBufferPipe;
	private long lastHeartbeatReceivedAt = -1;
	
	private NodeCallback nodeCallback;
	private ClusterNodePipeline clusterNodePipeline;
	
	public ClusterNode(SocketChannel socketChannel, NodeCallback nodeCallback) throws IOException {
		this.id = UUID.randomUUID().toString();
		
		this.nodeCallback = nodeCallback;
		this.socketChannel = socketChannel;
		this.socketChannel.configureBlocking(false);
		
		this.inputBufferPipe = Pipe.open();
		this.inputBufferPipe.sink().configureBlocking(false);
		this.inputBufferPipe.source().configureBlocking(false);
		
		this.outputBufferPipe = Pipe.open();
		this.outputBufferPipe.sink().configureBlocking(false);
		this.outputBufferPipe.source().configureBlocking(false);
		
		this.lastHeartbeatReceivedAt = System.currentTimeMillis();
	}
	
	public String getNodeName() {
		return this.nodeName;
	}
	
	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}
	
	public String getID() {
		return this.id;
	}
	
	public int getMaxSocketInputBufferSize() {
		return this.maxSocketInputBufferSize;
	}
	
	public int getOutputBufferBucketSize() {
		return this.outputBufferBucketSize;
	}
	
	public Pipe getInputBufferPipe() {
		return this.inputBufferPipe;
	}
	
	public Pipe getOutputBufferPipe() {
		return this.outputBufferPipe;
	}
	
	public void inputBufferPipeSinkWrite(ByteBuffer bucket) throws IOException {
		this.inputBufferPipe.sink().write(bucket);
	}
	
	public void inputBufferPipeSourceRead(ByteBuffer bucket) throws IOException {
		this.inputBufferPipe.source().read(bucket);
	}
	
	public void outputBufferPipeSinkWrite(ByteBuffer bucket) throws IOException {
		this.outputBufferPipe.sink().write(bucket);
	}
	
	public void outputBufferPipeSourceRead(ByteBuffer bucket) throws IOException {
		this.outputBufferPipe.source().read(bucket);
	}
	
	public SocketChannel getSocketChannel() {
		return this.socketChannel;
	}
	
	public int getMaxSocketOutputBufferSize() {
		return this.maxSocketOutputBufferSize;
	}
	
	public void setClusterNodePipeline(ClusterNodePipeline clusterNodePipeline) {
		this.clusterNodePipeline = clusterNodePipeline;
	}
	
	public ClusterNodePipeline getClusterNodePipeline() {
		return this.clusterNodePipeline;
	}
	
	public void kill() throws IOException {
		if(!isKilled()) {
			synchronized(killLock){		
				if(isKilled()) {
					return;
				}
				
				try {
					this.isKilled = true;
					
					if(this.inputBufferPipe != null) {
						if(this.inputBufferPipe.sink().isOpen()) this.inputBufferPipe.sink().close();
						if(this.inputBufferPipe.source().isOpen()) this.inputBufferPipe.source().close();
					}
					
					if(this.outputBufferPipe != null) {
						if(this.outputBufferPipe.source().isOpen()) this.outputBufferPipe.source().close();
						if(this.outputBufferPipe.sink().isOpen()) this.outputBufferPipe.sink().close();
					}
					
					if(this.socketChannel.isOpen()) this.socketChannel.close();
				} finally {
					this.nodeCallback.onKill();
				}
			}
		}
	}
	
	public void heartbeat(long sentAt) {
		this.lastHeartbeatReceivedAt = System.currentTimeMillis();
	}
	
	public boolean isKilled() {
		return this.isKilled;
	}
	
	public boolean isRunning() {
		if(this.socketChannel.isOpen() 
				&& this.socketChannel.isConnected() 
				&& (System.currentTimeMillis() - this.lastHeartbeatReceivedAt) < 5000) {
			return true;
		}
		
		try {
			return false;
		} finally {
			if(!this.isKilled()) {
				try {
					this.kill();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
