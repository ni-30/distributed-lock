package com.ni30.dlock.node;

import java.io.IOException;

/**
 * @author nitish.aryan (iitd.nitish@gmail.com)
 */
public abstract class PipelineIO<T> {
	
	public abstract T output() throws IOException;
	public abstract void input(T t) throws IOException;
}
