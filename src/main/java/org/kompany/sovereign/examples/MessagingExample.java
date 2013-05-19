package org.kompany.sovereign.examples;

import org.kompany.sovereign.Sovereign;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Demonstrate basic usage.
 * 
 * @author ypai
 * 
 */
public class MessagingExample {

    private static final Logger logger = LoggerFactory.getLogger(MessagingExample.class);

    public static void main(String[] args) throws Exception {
        /** init and start sovereign using builder **/
        Sovereign sovereign = Sovereign.builder().zkClient("localhost:2181", 15000).pathCache(1024, 8)
                .allCoreServices().build();
        sovereign.start();

        /** messaging example **/
        messagingExample(sovereign);

        /** sleep to allow examples to run for a bit **/
        Thread.sleep(120000);

        /** shutdown sovereign **/
        sovereign.stop();

        /** sleep a bit to observe observer callbacks **/
        Thread.sleep(10000);
    }

    public static void messagingExample(Sovereign sovereign) throws Exception {

    }
}
