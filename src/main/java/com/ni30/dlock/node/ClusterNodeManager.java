package com.ni30.dlock.node;

import com.ni30.dlock.Common;
import com.ni30.dlock.Constants;
import com.ni30.dlock.DLockCallback;
import com.ni30.dlock.TaskLooperService;
import com.ni30.dlock.task.CommandSenderTask;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author nitish.aryan
 */

public class ClusterNodeManager {
	private final TaskLooperService taskLooperService;
	private final Set<ClusterNode> nodeSet = ConcurrentHashMap.newKeySet();
	private final Map<String, ClusterNode> nodeMap = new ConcurrentHashMap<>();
	private ClusterNode leaderNode;
	private final String nodeName;
	private final ReentrantReadWriteLock leaderNodeLock = new ReentrantReadWriteLock();
	private final ReentrantReadWriteLock leaderMigrationLock = new ReentrantReadWriteLock();
	private final Map<String, DLockCallback> dLockCallbacks = new HashMap<>();
	private final Map<String, LockKeyInfo> lockedKeys = new ConcurrentHashMap<>();
	private final Map<String, LockRequestBucket> lockRequestBuckets = new HashMap<>();
	
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
	
	public DLockCallback getDLockCallback(String commandId) {
		return this.dLockCallbacks.get(commandId);
	}
	
	public void putDLockCallback(String commandId, DLockCallback callback) {
		this.dLockCallbacks.put(commandId, callback);
	}
	
	public void removeDLockCallback(String commandId) {
		this.dLockCallbacks.remove(commandId);
	}
	
	public void addLockedKey(String key, long leaseTime, String commandId) {
		LockKeyInfo info = new LockKeyInfo();
		info.startTime = System.currentTimeMillis();
		info.commandId = commandId;
		info.leaseTime = leaseTime;
		info.isLocked = true;
		
		lockedKeys.put(key, info);
	}
	
	public boolean isLockAcquiredByCurrentNode(String key) {
		if(lockedKeys.containsKey(key)) {
			LockKeyInfo v = lockedKeys.get(key);
			if(v.isLocked && (System.currentTimeMillis() - v.startTime) < v.leaseTime) {
				return true;
			}
		}
		
		return false;
	}
	
	public void markUnlocked(String key, String commandId) {
		LockKeyInfo v = lockedKeys.get(key);
		if(v.commandId.equals(commandId)) {
			v.isLocked = false;
		}
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
	
	public ClusterNode getNodeByName(String name) {
		return this.nodeMap.get(name);
	}
	
	public LockRequestBucket getLockRequestBucket(String key) {
		return lockRequestBuckets.compute(key, (k,v) -> v == null ? new LockRequestBucket() : v);
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
			nodeMap.put(this.node.getNodeName(), this.node);
		}
		
		@Override
		public void onKill() {
			nodeSet.remove(node);
			nodeMap.remove(this.node.getNodeName());
			if(leaderNode == null || leaderNode == node) {
				electLeader();
			}
		}
	}
	
	private static class LockKeyInfo {
		public String commandId;
		public long leaseTime;
		public long startTime;
		public boolean isLocked;
	}
}
