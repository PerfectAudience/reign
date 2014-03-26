package io.reign.examples;

import io.reign.Reign;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReignServer {
    private static final Logger logger = LoggerFactory.getLogger(ReignServer.class);

    public static void main(String[] args) throws Exception {
        /** init and start reign using builder **/
        Reign reign = Reign.maker().zkClient("localhost:2181", 30000).pathCache(1024, 8).get();
        reign.start();

        /** let server run **/
        Object obj = new Object();
        synchronized (obj) {
            obj.wait();
        }

    }
}
