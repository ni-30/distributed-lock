package com.ni30.dlock;

import java.util.UUID;

/**
 * @author nitish.aryan
 */
public class Common {
	
	public static String uuid() {
		return UUID.randomUUID().toString();
	}
	
	public static String convertToString(String... args) {
		StringBuilder sb = new StringBuilder().append("[");
		for(String arg : args) {
			sb.append(arg).append(", ");
		}
		
		if(sb.length() > 1) {
			sb.delete(sb.length() - 1, sb.length());
		}
		
		sb.append("]");
		
		return sb.toString();
	}
	
	public static String[] getCommand(byte[] byteCommand) {
		StringBuilder builder = new StringBuilder();
		for (byte b : byteCommand) {
			builder.append((char) b);
		}
		
		String[] command = Common.commandSplitter(builder.toString());
		
		return command;
	}
	
	// key, commandId, values...
	public static byte[] getByteCommand(Object... kv) {
		if(kv == null || kv.length < 2) {
			return null;
		}
		
		StringBuilder sb = new StringBuilder();
		sb.append(kv[0]);
		sb.append("/");
		
		for(int i=1; i<kv.length; i++) {
			sb.append(kv[i]).append(",");
		}
		sb.replace(sb.length() - 1, sb.length(), "");
		
		byte[] arr = new byte[sb.length()];
		for(int i=0; i < sb.length(); i++) {
			arr[i] = (byte) sb.charAt(i);
		}
		
		return arr;
	}
	
	private static String[] commandSplitter(String command) {
		String[] kv = command.trim().split("/");
		if(kv[0].isEmpty()) {
			return null; 
		}
		
		if(kv.length != 2) {
			return null;
		}
		
		String[] values = kv[1].split(",");
		if(values.length == 1) {
			return kv;
		}
		
		String[] keyVlaues = new String[values.length + 1];
		keyVlaues[0] = kv[0];
		for(int j=0; j < values.length; j++) {
			keyVlaues[1 + j] = values[j];
		}
		
		return keyVlaues;
	}
}
