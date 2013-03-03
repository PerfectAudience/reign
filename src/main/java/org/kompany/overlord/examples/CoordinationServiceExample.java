package org.kompany.overlord.examples;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.kompany.overlord.Sovereign;
import org.kompany.overlord.SovereignBuilder;
import org.kompany.overlord.coord.CoordinationService;
import org.kompany.overlord.coord.DistributedSemaphore;
import org.kompany.overlord.coord.ZkReentrantLock;
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
        /** init and start sovereign using builder **/
        Sovereign sovereign = (new SovereignBuilder()).zkConfig("localhost:2181", 15000).pathCache(1024, 8)
                .allCoreServices().build();
        sovereign.start();

        /** coordination service example **/
        // coordinationServiceReentrantLockExample(sovereign);
        // coordinationServiceExclusiveLockExample(sovereign);
        // coordinationServiceReadWriteLockExample(sovereign);
        // coordinationServiceFixedSemaphoreExample(sovereign);
        coordinationServiceConfiguredSemaphoreExample(sovereign);

        /** sleep to allow examples to run for a bit **/
        logger.info("Sleeping before shutting down Sovereign...");
        Thread.sleep(300000);

        /** shutdown sovereign **/
        sovereign.stop();

        /** sleep a bit to observe observer callbacks **/
        Thread.sleep(10000);
    }

    public static void coordinationServiceReentrantLockExample(Sovereign sovereign) throws Exception {
        // this is how you would normally get a service
        final CoordinationService coordService = (CoordinationService) sovereign.getService("coord");

        final int lockHoldTimeMillis = 30000;

        Thread t1 = new Thread() {
            @Override
            public void run() {
                Lock lock = coordService.getReentrantLock("node1", "examples-cluster", "exclusive_lock1");
                logger.info(this.getName() + ":  attempting to acquire lock...");
                lock.lock();
                try {
                    long sleepInterval = (long) (lockHoldTimeMillis * Math.random());
                    logger.info("{}:  acquired lock:  will hold for {} millis:  holdCount={}",
                            new Object[] { this.getName(), sleepInterval, ((ZkReentrantLock) lock).getHoldCount() });
                    Thread.sleep(sleepInterval);
                } catch (InterruptedException e) {
                    logger.info("Interrupted:  " + e, e);
                } finally {
                    lock.unlock();
                }
            }
        };
        t1.setName("T1");
        t1.setDaemon(true);
        t1.start();

        Thread t2 = new Thread() {
            @Override
            public void run() {
                Lock lock = coordService.getReentrantLock("node2", "examples-cluster", "exclusive_lock1");
                logger.info(this.getName() + ":  attempting to acquire lock...");
                lock.lock();
                try {
                    long sleepInterval = (long) (lockHoldTimeMillis * Math.random());
                    logger.info("{}:  acquired lock:  will hold for {} millis:  holdCount={}",
                            new Object[] { this.getName(), sleepInterval, ((ZkReentrantLock) lock).getHoldCount() });
                    Thread.sleep(sleepInterval);
                } catch (InterruptedException e) {
                    logger.info("Interrupted:  " + e, e);
                } finally {
                    lock.unlock();
                }
            }
        };
        t2.setName("T2");
        t2.setDaemon(true);
        t2.start();

        Thread t3 = new Thread() {
            @Override
            public void run() {
                Lock lock = coordService.getReentrantLock("node3", "examples-cluster", "exclusive_lock1");
                logger.info(this.getName() + ":  attempting to acquire lock...");
                lock.lock();
                try {
                    long sleepInterval = (long) (lockHoldTimeMillis * Math.random());
                    logger.info("{}:  acquired lock:  will hold for {} millis:  holdCount={}",
                            new Object[] { this.getName(), sleepInterval, ((ZkReentrantLock) lock).getHoldCount() });
                    Thread.sleep(sleepInterval);
                } catch (InterruptedException e) {
                    logger.info("Interrupted:  " + e, e);
                } finally {
                    lock.unlock();
                }
            }
        };
        t3.setName("T3");
        t3.setDaemon(true);
        t3.start();
    }

    public static void coordinationServiceConfiguredSemaphoreExample(Sovereign sovereign) throws Exception {
        // this is how you would normally get a service
        final CoordinationService coordService = (CoordinationService) sovereign.getService("coord");

        // configure semaphore
        coordService.setSemaphoreConf("examples-cluster", "semaphore2", 5);

        // wait a few seconds to make sure semaphore configuration is persisted
        // to ZK
        Thread.sleep(5000);

        final int lockHoldTimeMillis = 15000;

        Thread t1 = new Thread() {
            @Override
            public void run() {
                DistributedSemaphore semaphore = coordService.getConfiguredSemaphore("node1", "examples-cluster",
                        "semaphore2", 4, false);
                logger.info(this.getName() + ":  attempting to acquire lock...");

                int permitsToAcquire = 4;
                try {
                    semaphore.acquire(permitsToAcquire);
                    logger.info("{}:  acquired {} permit(s):  will hold for {} seconds...",
                            new Object[] { this.getName(), permitsToAcquire, lockHoldTimeMillis / 1000 });
                    Thread.sleep(lockHoldTimeMillis);
                } catch (InterruptedException e) {
                    logger.info("Interrupted:  " + e, e);
                } finally {
                    semaphore.release(permitsToAcquire);
                }
            }
        };
        t1.setName("T1");
        t1.setDaemon(true);
        t1.start();

        Thread t2 = new Thread() {
            @Override
            public void run() {
                DistributedSemaphore semaphore = coordService.getConfiguredSemaphore("node2", "examples-cluster",
                        "semaphore2", 4, false);
                logger.info(this.getName() + ":  attempting to acquire lock...");

                int permitsToAcquire = 2;
                try {
                    semaphore.acquire(permitsToAcquire);
                    logger.info("{}:  acquired {} permit(s):  will hold for {} seconds...",
                            new Object[] { this.getName(), permitsToAcquire, lockHoldTimeMillis / 1000 });
                    Thread.sleep(lockHoldTimeMillis);
                } catch (InterruptedException e) {
                    logger.info("Interrupted:  " + e, e);
                } finally {
                    semaphore.release(permitsToAcquire);
                }
            }
        };
        t2.setName("T2");
        t2.setDaemon(true);
        t2.start();

        Thread t3 = new Thread() {
            @Override
            public void run() {
                DistributedSemaphore semaphore = coordService.getConfiguredSemaphore("node3", "examples-cluster",
                        "semaphore2", 4, false);
                logger.info(this.getName() + ":  attempting to acquire lock...");

                try {
                    semaphore.acquire();
                    logger.info("{}:  acquired permit:  will hold for {} seconds...", this.getName(),
                            lockHoldTimeMillis / 1000);
                    Thread.sleep(lockHoldTimeMillis);
                } catch (InterruptedException e) {
                    logger.info("Interrupted:  " + e, e);
                } finally {
                    semaphore.release();
                }
            }
        };
        t3.setName("T3");
        t3.setDaemon(true);
        t3.start();

        Thread t4 = new Thread() {
            @Override
            public void run() {
                DistributedSemaphore semaphore = coordService.getConfiguredSemaphore("node4", "examples-cluster",
                        "semaphore2", 4, false);
                logger.info(this.getName() + ":  attempting to acquire lock...");

                try {
                    semaphore.acquire();
                    logger.info("{}:  acquired permit:  will hold for {} seconds...", this.getName(),
                            lockHoldTimeMillis / 1000);
                    Thread.sleep(lockHoldTimeMillis);
                } catch (InterruptedException e) {
                    logger.info("Interrupted:  " + e, e);
                } finally {
                    semaphore.release();
                }
            }
        };
        t4.setName("T4");
        t4.setDaemon(true);
        t4.start();

    }

    public static void coordinationServiceFixedSemaphoreExample(Sovereign sovereign) throws Exception {
        // this is how you would normally get a service
        final CoordinationService coordService = (CoordinationService) sovereign.getService("coord");

        final int lockHoldTimeMillis = 15000;

        Thread t1 = new Thread() {
            @Override
            public void run() {
                DistributedSemaphore semaphore = coordService.getFixedSemaphore("node1", "examples-cluster",
                        "semaphore1", 4);
                logger.info(this.getName() + ":  attempting to acquire lock...");

                int permitsToAcquire = 4;
                try {
                    semaphore.acquire(permitsToAcquire);
                    logger.info("{}:  acquired {} permit(s):  will hold for {} seconds...",
                            new Object[] { this.getName(), permitsToAcquire, lockHoldTimeMillis / 1000 });
                    Thread.sleep(lockHoldTimeMillis);
                } catch (InterruptedException e) {
                    logger.info("Interrupted:  " + e, e);
                } finally {
                    semaphore.release(permitsToAcquire);
                }
            }
        };
        t1.setName("T1");
        t1.setDaemon(true);
        t1.start();

        Thread t2 = new Thread() {
            @Override
            public void run() {
                DistributedSemaphore semaphore = coordService.getFixedSemaphore("node2", "examples-cluster",
                        "semaphore1", 4);
                logger.info(this.getName() + ":  attempting to acquire lock...");

                int permitsToAcquire = 2;
                try {
                    semaphore.acquire(permitsToAcquire);
                    logger.info("{}:  acquired {} permit(s):  will hold for {} seconds...",
                            new Object[] { this.getName(), permitsToAcquire, lockHoldTimeMillis / 1000 });
                    Thread.sleep(lockHoldTimeMillis);
                } catch (InterruptedException e) {
                    logger.info("Interrupted:  " + e, e);
                } finally {
                    semaphore.release(permitsToAcquire);
                }
            }
        };
        t2.setName("T2");
        t2.setDaemon(true);
        t2.start();

        Thread t3 = new Thread() {
            @Override
            public void run() {
                DistributedSemaphore semaphore = coordService.getFixedSemaphore("node3", "examples-cluster",
                        "semaphore1", 4);
                logger.info(this.getName() + ":  attempting to acquire lock...");

                try {
                    semaphore.acquire();
                    logger.info("{}:  acquired permit:  will hold for {} seconds...", this.getName(),
                            lockHoldTimeMillis / 1000);
                    Thread.sleep(lockHoldTimeMillis);
                } catch (InterruptedException e) {
                    logger.info("Interrupted:  " + e, e);
                } finally {
                    semaphore.release();
                }
            }
        };
        t3.setName("T3");
        t3.setDaemon(true);
        t3.start();

        Thread t4 = new Thread() {
            @Override
            public void run() {
                DistributedSemaphore semaphore = coordService.getFixedSemaphore("node4", "examples-cluster",
                        "semaphore1", 4);
                logger.info(this.getName() + ":  attempting to acquire lock...");

                try {
                    semaphore.acquire();
                    logger.info("{}:  acquired permit:  will hold for {} seconds...", this.getName(),
                            lockHoldTimeMillis / 1000);
                    Thread.sleep(lockHoldTimeMillis);
                } catch (InterruptedException e) {
                    logger.info("Interrupted:  " + e, e);
                } finally {
                    semaphore.release();
                }
            }
        };
        t4.setName("T4");
        t4.setDaemon(true);
        t4.start();

    }

    public static void coordinationServiceExclusiveLockExample(Sovereign sovereign) throws Exception {
        // this is how you would normally get a service
        final CoordinationService coordService = (CoordinationService) sovereign.getService("coord");

        final int lockHoldTimeMillis = 30000;

        Thread t1 = new Thread() {
            @Override
            public void run() {
                Lock lock = coordService.getLock("node1", "examples-cluster", "exclusive_lock1");
                logger.info(this.getName() + ":  attempting to acquire lock...");
                lock.lock();
                try {
                    long sleepInterval = (long) (lockHoldTimeMillis * Math.random());
                    logger.info("{}:  acquired lock:  will hold for {} millis...", this.getName(), sleepInterval);
                    Thread.sleep(sleepInterval);
                } catch (InterruptedException e) {
                    logger.info("Interrupted:  " + e, e);
                } finally {
                    lock.unlock();
                }
            }
        };
        t1.setName("T1");
        t1.setDaemon(true);
        t1.start();

        Thread t2 = new Thread() {
            @Override
            public void run() {
                Lock lock = coordService.getLock("node2", "examples-cluster", "exclusive_lock1");
                logger.info(this.getName() + ":  attempting to acquire lock...");
                lock.lock();
                try {
                    long sleepInterval = (long) (lockHoldTimeMillis * Math.random());
                    logger.info("{}:  acquired lock:  will hold for {} millis...", this.getName(), sleepInterval);
                    Thread.sleep(sleepInterval);
                } catch (InterruptedException e) {
                    logger.info("Interrupted:  " + e, e);
                } finally {
                    lock.unlock();
                }
            }
        };
        t2.setName("T2");
        t2.setDaemon(true);
        t2.start();

        Thread t3 = new Thread() {
            @Override
            public void run() {
                Lock lock = coordService.getLock("node3", "examples-cluster", "exclusive_lock1");
                logger.info(this.getName() + ":  attempting to acquire lock...");
                lock.lock();
                try {
                    long sleepInterval = (long) (lockHoldTimeMillis * Math.random());
                    logger.info("{}:  acquired lock:  will hold for {} millis...", this.getName(), sleepInterval);
                    Thread.sleep(sleepInterval);
                } catch (InterruptedException e) {
                    logger.info("Interrupted:  " + e, e);
                } finally {
                    lock.unlock();
                }
            }
        };
        t3.setName("T3");
        t3.setDaemon(true);
        t3.start();
    }

    public static void coordinationServiceReadWriteLockExample(Sovereign sovereign) throws Exception {
        // this is how you would normally get a service
        final CoordinationService coordService = (CoordinationService) sovereign.getService("coord");

        final int lockHoldTimeMillis = 30000;

        Thread t1 = new Thread() {
            @Override
            public void run() {
                ReadWriteLock rwLock = coordService.getReadWriteLock("node1", "examples-cluster", "rw_lock1");
                logger.info(this.getName() + ":  attempting to acquire lock...");
                rwLock.readLock().lock();
                try {
                    long sleepInterval = (long) (lockHoldTimeMillis * Math.random());
                    logger.info("{}:  acquired lock:  will hold for {} millis...", this.getName(), sleepInterval);
                    Thread.sleep(sleepInterval);
                } catch (InterruptedException e) {
                    logger.info("Interrupted:  " + e, e);
                } finally {
                    rwLock.readLock().unlock();
                }
            }
        };
        t1.setName("T1");
        t1.setDaemon(true);
        t1.start();

        Thread t2 = new Thread() {
            @Override
            public void run() {
                ReadWriteLock rwLock = coordService.getReadWriteLock("node2", "examples-cluster", "rw_lock1");
                logger.info(this.getName() + ":  attempting to acquire lock...");
                rwLock.readLock().lock();
                try {
                    long sleepInterval = (long) (lockHoldTimeMillis * Math.random());
                    logger.info("{}:  acquired lock:  will hold for {} millis...", this.getName(), sleepInterval);
                    Thread.sleep(sleepInterval);
                } catch (InterruptedException e) {
                    logger.info("Interrupted:  " + e, e);
                } finally {
                    rwLock.readLock().unlock();
                }
            }
        };
        t2.setName("T2");
        t2.setDaemon(true);
        t2.start();

        Thread t3 = new Thread() {
            @Override
            public void run() {
                ReadWriteLock rwLock = coordService.getReadWriteLock("node3", "examples-cluster", "rw_lock1");
                logger.info(this.getName() + ":  attempting to acquire lock...");
                rwLock.writeLock().lock();
                try {
                    long sleepInterval = (long) (lockHoldTimeMillis * Math.random());
                    logger.info("{}:  acquired lock:  will hold for {} millis...", this.getName(), sleepInterval);
                    Thread.sleep(sleepInterval);
                } catch (InterruptedException e) {
                    logger.info("Interrupted:  " + e, e);
                } finally {
                    rwLock.writeLock().unlock();
                }
            }
        };
        t3.setName("T3");
        t3.setDaemon(true);
        t3.start();

        Thread t4 = new Thread() {
            @Override
            public void run() {
                ReadWriteLock rwLock = coordService.getReadWriteLock("node4", "examples-cluster", "rw_lock1");
                logger.info(this.getName() + ":  attempting to acquire lock...");
                rwLock.writeLock().lock();
                try {
                    long sleepInterval = (long) (lockHoldTimeMillis * Math.random());
                    logger.info("{}:  acquired lock:  will hold for {} millis...", this.getName(), sleepInterval);
                    Thread.sleep(sleepInterval);
                } catch (InterruptedException e) {
                    logger.info("Interrupted:  " + e, e);
                } finally {
                    rwLock.writeLock().unlock();
                }
            }
        };
        t4.setName("T4");
        t4.setDaemon(true);
        t4.start();
    }
}
