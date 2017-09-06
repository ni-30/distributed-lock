package com.ni30.dlock;

import com.ni30.dlock.task.LoopTask;
import com.ni30.dlock.task.TaskLooper;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/**
 * @author nitish.aryan
 */
public class TaskLooperService {
	private final ReentrantReadWriteLock loopersLock = new ReentrantReadWriteLock();
	private final ExecutorService executorService;
	private final TaskLooper[] taskLoopers;
	private int pointer = -1;
	private final Object pointerLock = new Object();
	
	public TaskLooperService(int size) {
		this.executorService = Executors.newFixedThreadPool(size);
		this.taskLoopers = new TaskLooper[size];
	}
	
	public int getSize() {
		return this.taskLoopers.length;
	}
	
	public void start() {
		for(int i = 0; i < this.taskLoopers.length; i++) {
			TaskLooper taskLooper = new TaskLooper(i);
			this.executorService.submit(taskLooper);
			this.taskLoopers[i] = taskLooper;
		}
	}
	
	public void stop() throws InterruptedException {
		final ReadLock readLock = this.loopersLock.readLock();
		try {
			readLock.lockInterruptibly();
			for(TaskLooper tl : taskLoopers) {
				tl.stop();
			}
		} finally {
			readLock.unlock();
		}
	}
	
	public void addToNext(LoopTask task) {
		int next;
		synchronized(pointerLock) {
			this.pointer = (++pointer) % taskLoopers.length;
			next = this.pointer;
		}
		taskLoopers[next].addTask(task);
	}
	
	public void add(LoopTask... tasks) throws InterruptedException {
		final WriteLock writeLock = loopersLock.writeLock();
		try {
			writeLock.lockInterruptibly();
			Arrays.sort(taskLoopers, (a, b) -> {
				return a.getTotalWightage() > b.getTotalWightage() ? 1 : -1;
			});
		} finally {
			writeLock.unlock();
		}
		
		final ReadLock readLock = loopersLock.readLock();
		try {
			readLock.lockInterruptibly();
			int pointer = -1;
			for(LoopTask t : tasks) {
				taskLoopers[(++pointer) % taskLoopers.length].addTask(t);
			}
		} finally {
			readLock.unlock();
		}
	}
	
	public TaskLooper[] getLoopers(int loopSize) throws InterruptedException {
		final WriteLock writeLock = loopersLock.writeLock();
		try {
			writeLock.lockInterruptibly();
			Arrays.sort(taskLoopers, (a, b) -> {
				return a.getTotalWightage() > b.getTotalWightage() ? 1 : -1;
			});
		} finally {
			writeLock.unlock();
		}
		
		final ReadLock readLock = loopersLock.readLock();
		try {
			readLock.lockInterruptibly();
			final TaskLooper[] loopers = new TaskLooper[loopSize];
			for(int i = 0; i < loopSize && i < taskLoopers.length; i++) {
				loopers[i] = taskLoopers[i];
			}
			
			return loopers;
		} finally {
			readLock.unlock();
		}
	}
}
