package com.northscale.sample.generator;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;

/**
 *
 * This program is designed to place an interesting workload on a NorthScale
 * server.  By default, it will put a workload on a pool and bucket passed as
 * parameters.
 *
 * @author Matt Ingenthron <matt.ingenthron@northscale.com>
 * @version 0.5
 *
 * TODO: update version number to come out of git
 */
public class Main {

    private static int THREAD_COUNT = 4;
    private static long RUN_TIME = 60000;

    /**
     * Main entry point for the program.
     *
     * @param args The command line arguments.  The first parameter must be a
     * hostname or IP for any pool member.  The second parameter must be a 
     * bucket name.
     *
     */
    public static void main(String[] args) {
        if (args.length != 2) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE,
                    "Number of arguments incorrect.");
            System.err.println(
                    "usage: java -jar WorkloadGenerator.jar <pool> <bucket>");
        }

        MemcachedClient c = null;

        try {
            // Get a memcached client connected to several servers
            // TODO: make this work with buckets and pools
            c = new MemcachedClient(new BinaryConnectionFactory(),
                    AddrUtil.getAddresses("localhost:11210"));
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE,
                    "Unable to connect to memcached servers", ex);
        }

        // <editor-fold defaultstate="collapsed" desc="Create a daemon thread factory">
        ThreadFactory factory = new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setDaemon(true);
                return t;
            }
        };// </editor-fold>


        Executor workerPool = Executors.newFixedThreadPool(THREAD_COUNT,
                factory);
        for (int i = 0; i < THREAD_COUNT; i++) {
            workerPool.execute(new MemcapableWorker(c));
        }

        try {
            Thread.sleep(RUN_TIME);
        } catch (InterruptedException ex) {
            // doesn't matter if it's interrupted
        }

        System.exit(0);

    }
}
