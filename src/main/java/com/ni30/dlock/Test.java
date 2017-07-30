package com.ni30.dlock;

/**
 * @author nitish.aryan
 */
public class Test {
	public static void main( String[] args ) throws Exception {
        System.out.println( "TCP Distributed lock server started");
        DLockClient client = new DLockClient();
        client.open();
    }
}
