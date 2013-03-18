package org.kompany.sovereign.examples;

import org.kompany.sovereign.Sovereign;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Demonstrates basic usage of Sovereign's core services.
 * 
 * @author ypai
 * 
 */
public class QuickStartExample {

    private static final Logger logger = LoggerFactory.getLogger(QuickStartExample.class);

    public static void main(String[] args) throws Exception {
        /** init and start sovereign using builder **/
        Sovereign sovereign = Sovereign.builder().zkClient("localhost:2181", 15000).pathCache(1024, 8)
                .allCoreServices().build();
        sovereign.start();

        /** presence service example **/
        PresenceServiceExample.presenceServiceExample(sovereign);

        /** conf service example **/
        ConfServiceExample.confServiceExample(sovereign);

        /** coordination service example **/
        CoordinationServiceExample.coordinationServiceExclusiveLockExample(sovereign);
        CoordinationServiceExample.coordinationServiceReadWriteLockExample(sovereign);
        CoordinationServiceExample.coordinationServiceFixedSemaphoreExample(sovereign);

        /** sleep to allow examples to run for a bit **/
        Thread.sleep(60000);

        /** shutdown sovereign **/
        sovereign.stop();

        /** sleep a bit to observe observer callbacks **/
        Thread.sleep(10000);
    }
}
