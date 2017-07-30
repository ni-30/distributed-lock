package com.ni30.dlock.task;

/**
 * @author nitish.aryan (iitd.nitish@gmail.com)
 */
public class CurrentLoopTaskQueue {
	private final static ThreadLocal<TaskLooper> threadLocal = new ThreadLocal<>();
	
	public static void set(TaskLooper taskLooper) {
		if(threadLocal.get() != null) {
			throw new RuntimeException("task queue is set for the local thead - " + Thread.currentThread().getName());
		}
		
		threadLocal.set(taskLooper);
	}
	
	public static void enqueue(LoopTask taskExecutor) {
		if(taskExecutor == null) return;
		
		TaskLooper taskLooper = threadLocal.get();
		if(taskLooper == null) {
			throw new RuntimeException("task queue is set for the local thead - " + Thread.currentThread().getName());
		}
		
		taskLooper.addTask(taskExecutor);
	}
	
	public static void clear() {
		threadLocal.remove();
	}
}
