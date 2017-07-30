package com.ni30.dlock;

/**
 * @author nitish.aryan
 */

public interface SenderCallback {
	void preSending();
	void onSent();
	void onSendingFailure(Throwable e);
}
