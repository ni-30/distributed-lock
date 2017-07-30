package com.ni30.dlock.task;

import java.util.LinkedList;
import java.util.Queue;

/**
 * @author nitish.aryan (iitd.nitish@gmail.com)
 */

public class TaskLooper implements Runnable {
	private final int position;
	private Thread currentThread;
	private Object enqueueLock = new Object();
	private Object dequeueLock = new Object();
	private final Queue<LoopTask> tasksQueue = new LinkedList<>();
	private int totalWeightage = 0;
	private Object totalWeightageLock = new Object();
	
	public TaskLooper(int position) {
		this.position = position;
	}
	
	public int getPosition() {
		return this.position;
	}
	
	public boolean isRunning() {
		return currentThread != null;
	}
	
	public int getTotalWightage() {
		return this.totalWeightage;
	}
	
	public void addTask(LoopTask task) {
		synchronized(this.enqueueLock) {
			this.tasksQueue.add(task);
			synchronized(totalWeightageLock) {
				this.totalWeightage += task.getWeightage();
			}
		}
	}
	
	@Override
	public void run() {
		try {
			this.currentThread = Thread.currentThread();
			CurrentLoopTaskQueue.set(this); 
			while(!Thread.currentThread().isInterrupted()) {
				LoopTask task;
				synchronized(this.dequeueLock) {
					if(this.tasksQueue.isEmpty()) {
						continue;
					}
					task = this.tasksQueue.poll();
				}
				
				if(task != null) {
					try {
						task.execute();
					} catch(Exception ignore) {
						ignore.printStackTrace();
					} finally {
						synchronized(totalWeightageLock) {
							this.totalWeightage -= task.getWeightage();
						}
					}
				}
			}
			
		} finally {
			this.currentThread = null;
		}
	}
	
	public void stop() {
		if(this.currentThread != null) {
			this.currentThread.interrupt();
		}
	}
}
