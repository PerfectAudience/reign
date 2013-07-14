package io.reign.example;

import io.reign.Reign;
import io.reign.coord.CoordinationService;
import io.reign.coord.DistributedReadWriteLock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class CoordinationServiceExample {
    private static final Logger logger = LoggerFactory.getLogger(CoordinationServiceExample.class);

    public static void main(String[] args) throws Exception {
        /** init and start reign using builder **/
        Reign reign = Reign.maker().zkClient("54.234.125.39:2181", 15000).pathCache(1024, 8).core().get();
        reign.start();

        /** coordination service examples **/
        coordinationServiceReadWriteLockExample(reign);

        /** sleep to allow examples to run for a bit **/
        logger.info("Sleeping before shutting down...");
        Thread.sleep(600000);

        /** shutdown reign **/
        reign.stop();

        /** sleep a bit to observe observer callbacks **/
        Thread.sleep(10000);
    }

    public static void coordinationServiceReadWriteLockExample(Reign reign) throws Exception {
        // this is how you would normally get a service
        final CoordinationService coordService = (CoordinationService) reign.getService("coord");

        final int lockHoldTimeMillis = 30000;

        final AtomicInteger readLocksHeld = new AtomicInteger(0);
        final AtomicInteger writeLocksHeld = new AtomicInteger(0);
        final String lockname = "lockname21";

        Thread t3 = new Thread() {
            @Override
            public void run() {
                DistributedReadWriteLock rwLock = coordService.getReadWriteLock("examples", lockname);
                logger.info(this.getName() + ":  attempting to acquire lock...");
                rwLock.writeLock().lock();
                System.out.println("***** WRITE LOCK *****");
                try {
                    writeLocksHeld.incrementAndGet();
                    long sleepInterval = 50000;
                    logger.info(
                            "{}:  acquired WRITE lock:  will hold for {} millis:  readLocksHeld={}; writeLocksHeld={}",
                            new Object[] { this.getName(), sleepInterval, readLocksHeld.get(), writeLocksHeld.get() });
                    Thread.sleep(sleepInterval);
                } catch (InterruptedException e) {
                    logger.info("Interrupted:  " + e, e);
                } finally {
                    System.out.println("***** RELEASE WRITE LOCK *****");

                    writeLocksHeld.decrementAndGet();
                    rwLock.writeLock().unlock();
                    rwLock.destroy();
                }
            }
        };
        t3.setName("T3");
        t3.setDaemon(true);
        t3.start();

        Thread t1 = new Thread() {
            @Override
            public void run() {
                for (int i = 0; i < 100; i++) {
                    DistributedReadWriteLock rwLock = coordService.getReadWriteLock("examples", lockname);
                    logger.info(this.getName() + ":  attempting to acquire lock...");

                    boolean acquired = false;
                    try {
                        acquired = rwLock.readLock().tryLock(5000l, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                        acquired = false;
                    }
                    if (acquired) {
                        System.out.println("***** READ LOCK *****");
                        try {
                            readLocksHeld.incrementAndGet();
                            long sleepInterval = (long) (2000);
                            logger.info(
                                    "{}:  acquired READ lock:  will hold for {} millis:  readLocksHeld={}; writeLocksHeld={}",
                                    new Object[] { this.getName(), sleepInterval, readLocksHeld.get(),
                                            writeLocksHeld.get() });
                            Thread.sleep(sleepInterval);
                        } catch (InterruptedException e) {
                            logger.info("Interrupted:  " + e, e);
                        } finally {
                            readLocksHeld.decrementAndGet();
                            rwLock.readLock().unlock();
                            rwLock.destroy();
                        }
                    } else {
                        System.out.println("**** UNABLE TO GET THE LOCK ****");
                        long sleepInterval = (long) (2000);
                        try {
                            Thread.sleep(sleepInterval);
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                } // for
            }
        };
        t1.setName("T1");
        t1.setDaemon(true);
        t1.start();
        Thread.sleep(6000);

    }
}
