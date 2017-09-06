package com.ni30.dlock;

/**
 * @author nitish.aryan (iitd.nitish@gmail.com)
 */

public class Constants {
	public static final String
		HANDSHAKE_COMMAND_KEY = "handshake",
		HEART_BEAT_COMMAND_KEY = "heartbeat",
		ELECT_LEADER_COMMAND_KEY = "elect_leader",
		NEW_LEADER_COMMAND_KEY = "new_leader",
		TRY_LOCK_COMMAND_KEY = "try_lock",
		LOCK_GRANTED_COMMAND_KEY = "lock_granted",
		UNLOCK_COMMAND_KEY = "unlock";
}
