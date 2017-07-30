/**
 * @author nitish.aryan
 */
package com.ni30.dlock;


public class App {
	
	public static void main( String[] args ) throws Exception {
        System.out.println( "TCP Distributed lock server started");
        DLockBootstrap app = new DLockBootstrap();
        app.start();
    }

}
