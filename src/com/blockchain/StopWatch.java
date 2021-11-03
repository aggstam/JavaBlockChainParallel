// -------------------------------------------------------------
//
// This is a stopwatch utility, used to calculate elapsed time of
// methods, measured in seconds.
// Available actions: Start and Stop.
//
// Author: Aggelos Stamatiou, November 2019
//
// --------------------------------------------------------------

package com.blockchain;

import java.util.logging.Logger;

public class StopWatch {

    private static Logger logger = Logger.getLogger(StopWatch.class.getName());
    private long startTime;
    private long endTime;

    public StopWatch() {}

    public void start(String processName) {
        logger.info("Process " + processName +" started.");
        this.startTime = System.nanoTime();
    }

    public void stop(String processName) {
        this.endTime = System.nanoTime();
        logger.info("Process " + processName +" finished. Total time elapsed: " + (float)(endTime-startTime)/1000000000 +" seconds");
    }
}
