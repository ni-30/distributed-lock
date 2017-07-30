package com.ni30.dlock.task;

/**
 * @author nitish.aryan (iitd.nitish@gmail.com)
 */
public abstract class LoopTask {
	private int weightage = 0;
	
	public void setWeightage(int weightage) {
		this.weightage = weightage;
	}
	
	public int getWeightage() {
		return this.weightage;
	}
	
	public abstract void execute() throws Exception;
}
