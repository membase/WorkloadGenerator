package com.northscale.sample.generator;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.spy.memcached.MemcachedClient;

/**
 *
 * @author Matt Ingenthron <matt.ingenthron@sun.com>
 */
class MemcapableWorker implements Runnable {

    private final MemcachedClient c;
    private final Random r = new Random();
    ArrayList availableKeys = new ArrayList();

    public MemcapableWorker(MemcachedClient client) {
        Thread.currentThread().setName("memcapableWorker");
        c = client;
    }

    public void run() {
        while (true) {
            try {
                Thread.sleep(r.lrandom(100, 1000));
            } catch (InterruptedException ex) {
                // Interruptions don't really matter to this thread
            }

            int op = r.random(1, 5);
            // TODO: implement incr and decr
            switch (op) {
                case 1: doAnAdd();
                case 2: doAnAppend();
                case 3: doAPrepend();
                case 4: doASet();
                case 5: doADelete();
                // TODO: implement incr/decr
//                case 6: doAnIncr();
//                case 7: doADecr();

            }
        }
    }

    /**
     * With an already defined MemcachedClient, perform a set operation with a
     * random set of data.
     *
     */
    private void doASet() {
            String key = r.makeAString(5, 250);
            Integer exp = r.random(0, 60*60*24*30);
            String value = r.makeAString(1024, 1024*1024);
            Logger.getLogger(MemcapableWorker.class.getName()).log(Level.INFO,
                    "key: " + key + " expiration: " + exp);
            c.set(key, exp, value);
            availableKeys.add(key);
    }

    private void doAnAdd() {
        String key = r.makeAString(5, 250);
            Integer exp = r.random(0, 60*60*24*30);
            String value = r.makeAString(1024, 1024*1024);
            Logger.getLogger(MemcapableWorker.class.getName()).log(Level.INFO,
                    "key: " + key + " expiration: " + exp);
            c.add(key, exp, value);
            availableKeys.add(key);
    }

    private void doAnAppend() {
        if (availableKeys.size() == 0)
            return;
        c.append(r.lrandom(1, Long.MAX_VALUE),
                availableKeys.get(r.random(0, availableKeys.size() - 1  )).toString(),
                (Object)r.makeAString(10, 100));
    }

    private void doAPrepend() {
        if (availableKeys.size() == 0)
            return;
        c.prepend(r.lrandom(1, Long.MAX_VALUE),
                availableKeys.get(r.random(0, availableKeys.size() - 1 )).toString(),
                (Object)r.makeAString(10, 100));
    }

    private void doAnIncr() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private void doADecr() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private void doADelete() {
        if (availableKeys.size() == 0)
            return;
        int idx = r.random(0, availableKeys.size() - 1);
        availableKeys.remove(idx);
        try {
            c.delete(availableKeys.get(idx).toString());
        }
        catch (Exception ex) {
            //
        }
        
    }


}
