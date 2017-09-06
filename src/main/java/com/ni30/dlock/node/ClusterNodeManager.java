package com.ni30.dlock.node;

import com.ni30.dlock.Common;
import com.ni30.dlock.Constants;
import com.ni30.dlock.TaskLooperService;
import com.ni30.dlock.task.CommandSenderTask;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author nitish.aryan
 */

public class ClusterNodeManager {
	private final TaskLooperService taskLooperService;
	private final Set<ClusterNode> nodeSet = ConcurrentHashMap.newKeySet();
	private ClusterNode leaderNode;
	private final String nodeName;
	private final ReentrantReadWriteLock leaderNodeLock = new ReentrantReadWriteLock();
	private final ReentrantReadWriteLock leaderMigrationLock = new ReentrantReadWriteLock();
	
	public ClusterNodeManager(String nodeName, TaskLooperService taskLooperService) {
		this.nodeName = nodeName;
		this.taskLooperService = taskLooperService;
	}
	
	public ClusterNode newNode(SocketChannel socketChannel) throws IOException {
		NodeCallbackImpl nodeCallback = new NodeCallbackImpl();
		ClusterNode clusterNode = new ClusterNode(socketChannel, nodeCallback);
		nodeCallback.setClusterNode(clusterNode);
		return clusterNode;
	}
	
	public String getNodeName() {
		return this.nodeName;
	}
	
	public ClusterNode getLeaderNode() {
		ReentrantReadWriteLock.ReadLock lock1 = leaderNodeLock.readLock();
		ReentrantReadWriteLock.ReadLock lock2 = leaderMigrationLock.readLock();
		lock1.lock();
		lock2.lock();
		try {
			return this.leaderNode;
		} finally {
			lock1.unlock();
			lock2.unlock();
		}
	}
	
	public void onNewLeaderSelection(String newLeaderName) {
		ReentrantReadWriteLock.WriteLock lock1 = leaderNodeLock.writeLock();
		lock1.lock();
		
		try {
			Iterator<ClusterNode> iterator = nodeSet.iterator();
			while(iterator.hasNext()) {
				ClusterNode n = iterator.next();
				if(n.getNodeName().equals(newLeaderName) 
						&& (leaderNode == null 
							|| !leaderNode.isRunning() 
							|| n.getNodeName().hashCode() > leaderNode.getNodeName().hashCode())) {
					
					String existingLeaderNodeName = leaderNode == null ? null : leaderNode.getNodeName();
					leaderNode = n;
					if(nodeName.equals(existingLeaderNodeName)) {
						migrateLeaderData();
					}
					break;
				}
			}
		} finally {
			lock1.unlock();
		}
	}
	
	public synchronized void electLeader() {
		List<CommandSenderTask> cst = new ArrayList<>();
		
		Iterator<ClusterNode> iterator = nodeSet.iterator();
		while(iterator.hasNext()) {
			ClusterNode n = iterator.next();
			
			int selfHashCode = nodeName.hashCode();
			int nodeHashCode = n.hashCode();
			
			if(nodeHashCode <= selfHashCode) {
				continue;
			}
			
			if(!n.isRunning()) {
				continue;
			}
			
			CommandSenderTask commandSenderTask = new CommandSenderTask(n, new Object[]{Constants.ELECT_LEADER_COMMAND_KEY, Common.uuid()}, null);
			cst.add(commandSenderTask);
		}
		
		if(cst.isEmpty()) {
			iterator = nodeSet.iterator();
			while(iterator.hasNext()) {
				ClusterNode n = iterator.next();
				
				if(!n.isRunning()) {
					continue;
				}
				
				CommandSenderTask commandSenderTask = new CommandSenderTask(n, new Object[]{Constants.NEW_LEADER_COMMAND_KEY, Common.uuid()}, null);
				cst.add(commandSenderTask);
			}
		}
		
		while(true) {
			try {
				taskLooperService.add((CommandSenderTask[]) cst.toArray());
				break;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void migrateLeaderData() {
		ReentrantReadWriteLock.WriteLock lock2 = leaderMigrationLock.writeLock();
		lock2.lock();
		
		try {
			// TODO : migrate local leader data to newly elected leader
		} finally {
			lock2.unlock();
		}
	}
	
	private class NodeCallbackImpl implements NodeCallback {
		private ClusterNode node;
		
		private void setClusterNode(ClusterNode node) {
			this.node = node;
		}
		
		@Override
		public void onInit() {
			nodeSet.add(this.node);
		}
		
		@Override
		public void onKill() {
			nodeSet.remove(node);
			if(leaderNode == null || leaderNode == node) {
				electLeader();
			}
		}
	}
}
