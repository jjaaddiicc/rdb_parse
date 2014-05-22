/**
 * @author 	Jadic
 * @created 2014-2-28
 */
package com.jadic.utils;

import java.lang.Thread.UncaughtExceptionHandler;

import org.apache.log4j.Logger;

public class MyExceptionHandler implements UncaughtExceptionHandler {
    
    private Logger logger = Logger.getLogger(MyExceptionHandler.class);

	@Override
	public void uncaughtException(Thread t, Throwable e) {
	    logger.error("An exception has been captured\n");
	    logger.error(String.format("Thread:%s\n", t.getName()));
	    logger.error(String.format("Exception: %s: %s:\n", e.getClass().getName(), KKTool.getExceptionTip(e)));
	    logger.error(String.format("Thread status:%s\n", t.getState()));
	}

}
