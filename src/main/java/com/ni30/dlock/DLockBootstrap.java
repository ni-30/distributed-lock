package com.ni30.dlock;

import com.ni30.dlock.node.ClusterNode;
import com.ni30.dlock.node.ClusterNodeBufferPipeline;
import com.ni30.dlock.node.ClusterNodePipeline;
import com.ni30.dlock.node.PipelineClosedException;
import com.ni30.dlock.task.ClusterNodeHeartbeatTask;
import com.ni30.dlock.task.CurrentLoopTaskQueue;
import com.ni30.dlock.task.InputBufferWriterTask;
import com.ni30.dlock.task.LoopTask;
import com.ni30.dlock.task.OutputBufferWriterTask;
import com.ni30.dlock.task.OutputCommandByteProcessorTask;
import com.ni30.dlock.task.TaskLooper;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.channels.AlreadyBoundException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Properties;

/**
 * @author nitish.aryan (iitd.nitish@gmail.com)
 */
class DLockBootstrap {
	private final String host;
	private final int minPortNumber;
	private final int maxPortNumber;
	private final int serverSocketPort;
	private final ServerSocketChannel serverSocketChannel;
	public final LockService lockService;
	private final TaskLooperService taskLooperService;
    
	public DLockBootstrap() throws Exception {
		this(new Properties());
	}
	
	public DLockBootstrap(Properties properties) throws Exception {
		int threadPoolSize = Integer.parseInt(properties.getProperty("dlock.threadPoolSize", (Runtime.getRuntime().availableProcessors() * 2) + ""));
		if(threadPoolSize < 2) {
			threadPoolSize = 2;
		}
		this.taskLooperService = new TaskLooperService(threadPoolSize);
		
		this.host = properties.getProperty("dlock.host", "127.0.0.1");
		this.serverSocketChannel = ServerSocketChannel.open();
		
		this.minPortNumber = Integer.parseInt(properties.getProperty("dlock.port.min", "4900").toString());
		this.maxPortNumber = Integer.parseInt(properties.getProperty("dlock.port.max", "5000").toString());
		
		int p = this.minPortNumber;
		Exception e = null;
        for(; p <= this.maxPortNumber; p++) {
        	try {
                this.serverSocketChannel.bind(new InetSocketAddress(this.host, p), 10);
                break;
            } catch (BindException | AlreadyBoundException e2) {
            	e = e2;
            }
        }
        
		if(p == this.maxPortNumber + 1) {
			throw e;
		}
		
		this.serverSocketChannel.configureBlocking(false);
        this.serverSocketPort = p;
        this.lockService = new LockService(this.host + ":" + this.serverSocketPort , this.taskLooperService);
        
        System.out.println("DLock listening on port: " + this.serverSocketPort);
	}
	
	public LockService getLockService() {
		return this.lockService;
	}
	
	public void start() throws Exception {
		this.taskLooperService.start();
		this.taskLooperService.add(new SocketChannelAcceptTask());
		this.taskLooperService.add(new ExistingScoketChannelConnectionTask());
	}
	
	protected void loopIn(ClusterNode clusterNode) throws Exception {
		LoopTask task = new AddPipeline(clusterNode);
		this.taskLooperService.add(task);
	}
	
	public void stop() throws Exception {
		if(this.serverSocketChannel.isOpen()) {
			try {
				this.serverSocketChannel.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		this.taskLooperService.stop();
	}
	
	protected class ExistingScoketChannelConnectionTask extends LoopTask {
		@Override
		public void execute() throws Exception {
			for(int p = minPortNumber; p <= maxPortNumber; p--) {
				if(p == serverSocketPort) continue;
				
				SocketChannel clusterNodeSocketChannel = null;
				try {
					clusterNodeSocketChannel = SocketChannel.open();
					clusterNodeSocketChannel.connect(new InetSocketAddress(host, p));
					
					if(!clusterNodeSocketChannel.isConnected()) {
						throw new RuntimeException("socket channel is not connected to host - " + host + " and port - " + p);
					}
					
					final ClusterNode clusterNode = new ClusterNode(clusterNodeSocketChannel);
					loopIn(clusterNode);
				} catch(Exception e) {
					e.printStackTrace();
					if(clusterNodeSocketChannel != null && clusterNodeSocketChannel.isOpen()) {
						try {
							clusterNodeSocketChannel.close();
						} catch (IOException ignore) {
							ignore.printStackTrace();
						}
					}
				}
			}
		}
	}
	
	protected class SocketChannelAcceptTask extends LoopTask {
		@Override
		public void execute() throws Exception {
			if(!serverSocketChannel.isOpen()) {
				return;
			}
			
			SocketChannel clusterNodeSocketChannel = null;
			try {
				while(true) {
					clusterNodeSocketChannel = serverSocketChannel.accept();
					if(clusterNodeSocketChannel == null) {
						return;
					}
					
					final ClusterNode clusterNode = new ClusterNode(clusterNodeSocketChannel);
					loopIn(clusterNode);
				}
			} catch (Exception e) {
				e.printStackTrace();
				if(clusterNodeSocketChannel != null && clusterNodeSocketChannel.isOpen()) {
					try {
						clusterNodeSocketChannel.close();
					} catch (IOException ignore) {
						ignore.printStackTrace();
					}
				}
			} finally {
				if(serverSocketChannel.isOpen()) {
					CurrentLoopTaskQueue.enqueue(this);
				}
			}
		}
	}
	
	protected class AddPipeline extends LoopTask {
		private final ClusterNode clusterNode;
		
		public AddPipeline(ClusterNode clusterNode) {
			this.clusterNode = clusterNode;
		}

		@Override
		public void execute() throws Exception {
			ClusterNodeBufferPipeline clusterNodeBufferPipeline = new ClusterNodeBufferPipeline(this.clusterNode);
			ClusterNodePipeline clusterNodePipeline = new ClusterNodePipeline(this.clusterNode);
			
			ClusterNodeHeartbeatTask clusterNodeHeartbeatTask = new ClusterNodeHeartbeatTask(clusterNodePipeline);
			clusterNodeHeartbeatTask.setWeightage(1);
			InputBufferWriterTask inputBufferWriterTask = new InputBufferWriterTask(clusterNodeBufferPipeline);
			inputBufferWriterTask.setWeightage(2);
			OutputBufferWriterTask outputBufferWriterTask = new OutputBufferWriterTask(clusterNodeBufferPipeline);
			outputBufferWriterTask.setWeightage(2);
			NodeHandshakeAndDeployTask handshakeTask = new NodeHandshakeAndDeployTask(clusterNodePipeline, lockService, serverSocketPort);
			handshakeTask.setWeightage(3);
			
			taskLooperService.add(clusterNodeHeartbeatTask, handshakeTask, inputBufferWriterTask, outputBufferWriterTask);
		}
	}
	
	protected class NodeHandshakeAndDeployTask extends LoopTask {
		private final ClusterNodePipeline clusterNodePipeline;
		private final int serverSocketPort;
		private long startedAt = -1;
		private boolean isSent = false;
		private LockService lockContainer;
		
		public NodeHandshakeAndDeployTask(ClusterNodePipeline clusterNodePipeline, LockService lockContainer, int port) {
			this.clusterNodePipeline = clusterNodePipeline;
			this.lockContainer = lockContainer;
			this.serverSocketPort = port;
			this.setWeightage(2);
		}
		
		@Override
		public void execute() throws Exception {
			if(startedAt == -1) {
				startedAt = System.currentTimeMillis();
			}
			
			boolean enqueueNextTime = true;
			try {
				if(!isSent) {
					final byte[] handshakeBytes = Common.getByteCommand(Constants.HANDSHAKE_COMMAND_KEY, Common.uuid(), host, serverSocketPort);
					this.clusterNodePipeline.input(handshakeBytes);
					isSent = true;
				}
				
				byte[] output = clusterNodePipeline.output();
				if(output == null) {
					if(System.currentTimeMillis() - startedAt > 5000) {
						enqueueNextTime = false;
						this.clusterNodePipeline.getClusterNode().kill();
					}
					return;
				}
				
				final String[] command = Common.getCommand(output);
				if(command == null) return;
				
				if(command.length != 4 || !Constants.HANDSHAKE_COMMAND_KEY.equals(command[0])) {
					enqueueNextTime = false;
					this.clusterNodePipeline.getClusterNode().kill();
					throw new InvalidCommandException("invalid handshake command received - " + Common.convertToString(command));
				}
				
				lockService.add(clusterNodePipeline);
				
				enqueueNextTime = false;
				
				this.clusterNodePipeline.getClusterNode().setNodeName(command[2] + ":" + command[3]);
				
				final TaskLooper[] loopers = taskLooperService.getLoopers(taskLooperService.getSize());
				
				for(TaskLooper l : loopers) {
					if(l == null) break;
					
					OutputCommandByteProcessorTask task = new OutputCommandByteProcessorTask(clusterNodePipeline, lockContainer);
					task.setWeightage(3);
					l.addTask(task);
				}
			} catch (PipelineClosedException e) {
				enqueueNextTime = false;
				e.printStackTrace();
			} finally {
				if(enqueueNextTime) {
					CurrentLoopTaskQueue.enqueue(this);
				}
			}
		}
		
	}

}
