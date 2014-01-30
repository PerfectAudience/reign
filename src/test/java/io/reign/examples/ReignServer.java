package io.reign.examples;

import io.reign.Reign;

public class ReignServer {
    public static void main(String[] args) throws Exception {
        /** init and start reign using builder **/
        Reign reign = Reign.maker().zkClient("localhost:2181", 30000).pathCache(1024, 8).core().get();
        reign.start();

        /** sleep to allow examples to run for a bit **/
        Object obj = new Object();
        synchronized (obj) {
            obj.wait();
        }

        /** shutdown reign **/
        reign.stop();

    }
}
