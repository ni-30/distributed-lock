package com.ni30.dlock.node;

/**
 * @author nitish.aryan
 */
public class PipelineClosedException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public PipelineClosedException(String string) {
		super(string);
	}

	public PipelineClosedException(String string, Throwable e) {
		super(string, e);
	}

}
