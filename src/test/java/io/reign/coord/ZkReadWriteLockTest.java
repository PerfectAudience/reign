package io.reign.coord;

import static org.junit.Assert.assertTrue;
import io.reign.MasterTestSuite;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import org.junit.Before;
import org.junit.Test;

public class ZkReadWriteLockTest {

    private CoordinationService coordinationService;

    @Before
    public void setUp() throws Exception {

        coordinationService = MasterTestSuite.getReign().getService("coord");

    }

    @Test
    public void testMixedLocks() throws Exception {

        // use StringBuffer because it is synchronized and thread-safe
        final StringBuffer sb = new StringBuffer();

        final AtomicInteger currentIndex = new AtomicInteger(-1);
        final AtomicInteger index = new AtomicInteger(-1);

        // threads to simulate multiple processes
        Thread t1 = new Thread() {
            @Override
            public void run() {
                DistributedReadWriteLock rwLock = coordinationService.getReadWriteLock("clusterA", "test-lock-1");
                Lock lock = rwLock.readLock();
                lock.lock();
                try {
                    currentIndex.incrementAndGet();
                    // synchronized (this) {
                    // this.notifyAll();
                    // }
                    // Thread.sleep(1000);
                    sb.append("1");
                } catch (Exception e) {
                } finally {
                    lock.unlock();
                    rwLock.destroy();
                }
            }
        };
        Thread t2 = new Thread() {
            @Override
            public void run() {
                DistributedReadWriteLock rwLock = coordinationService.getReadWriteLock("clusterA", "test-lock-1");
                Lock lock = rwLock.readLock();
                lock.lock();
                try {
                    currentIndex.incrementAndGet();
                    // synchronized (this) {
                    // this.notifyAll();
                    // }
                    // Thread.sleep(1000);
                    sb.append("2");
                } catch (Exception e) {
                } finally {
                    lock.unlock();
                    rwLock.destroy();
                }
            }
        };
        Thread t3 = new Thread() {
            @Override
            public void run() {
                DistributedReadWriteLock rwLock = coordinationService.getReadWriteLock("clusterA", "test-lock-1");
                Lock lock = rwLock.readLock();
                lock.lock();
                try {
                    currentIndex.incrementAndGet();
                    // synchronized (this) {
                    // this.notifyAll();
                    // }
                    // Thread.sleep(1000);
                    sb.append("3");
                } catch (Exception e) {
                } finally {
                    lock.unlock();
                    rwLock.destroy();
                }
            }
        };
        Thread t4 = new Thread() {
            @Override
            public void run() {
                DistributedReadWriteLock rwLock = coordinationService.getReadWriteLock("clusterA", "test-lock-1");
                Lock lock = rwLock.writeLock();
                lock.lock();
                try {
                    index.set(currentIndex.incrementAndGet());
                    // synchronized (this) {
                    // this.notifyAll();
                    // }
                    // Thread.sleep(1000);
                    sb.append("4");
                } catch (Exception e) {
                } finally {
                    lock.unlock();
                    rwLock.destroy();
                }
            }
        };

        // long start = System.currentTimeMillis();
        t1.start();
        // synchronized (t1) {
        // t1.wait();
        // }
        t2.start();
        // synchronized (t2) {
        // t2.wait();
        // }
        t3.start();
        // synchronized (t3) {
        // t3.wait();
        // }
        t4.start();
        // synchronized (t4) {
        // t4.wait();
        // }
        // long end = System.currentTimeMillis();

        // assertTrue("No contention for read locks so should not have waited sequentially:  elapsedMillis="
        // + (end - start), end - start < 3000);
        long start = System.currentTimeMillis();
        while (sb.length() < 4 && System.currentTimeMillis() - start < 10000) {
            Thread.sleep(500);
        }

        assertTrue("Unexpected value:  " + sb, sb.toString().length() == 4);
        assertTrue("Unexpected value at write lock thread index:  string=" + sb + "; index=" + index.get(),
                "4".equals(sb.toString().substring(index.get(), index.get() + 1)));

    }

    @Test
    public void testReadLock() throws Exception {

        // use StringBuffer because it is synchronized and thread-safe
        final StringBuffer sb = new StringBuffer();

        // threads to simulate multiple processes
        Thread t1 = new Thread() {
            @Override
            public void run() {
                DistributedReadWriteLock rwLock = coordinationService.getReadWriteLock("clusterA", "test-lock-1");
                Lock lock = rwLock.readLock();
                lock.lock();
                try {
                    synchronized (this) {
                        this.notifyAll();
                    }
                    Thread.sleep(1000);
                    sb.append("1");
                } catch (Exception e) {
                } finally {
                    lock.unlock();
                    rwLock.destroy();
                }
            }
        };
        Thread t2 = new Thread() {
            @Override
            public void run() {
                DistributedReadWriteLock rwLock = coordinationService.getReadWriteLock("clusterA", "test-lock-1");
                Lock lock = rwLock.readLock();
                lock.lock();
                try {
                    synchronized (this) {
                        this.notifyAll();
                    }
                    Thread.sleep(1000);
                    sb.append("2");
                } catch (Exception e) {
                } finally {
                    lock.unlock();
                    rwLock.destroy();
                }
            }
        };
        Thread t3 = new Thread() {
            @Override
            public void run() {
                DistributedReadWriteLock rwLock = coordinationService.getReadWriteLock("clusterA", "test-lock-1");
                Lock lock = rwLock.readLock();
                lock.lock();
                try {
                    synchronized (this) {
                        this.notifyAll();
                    }
                    Thread.sleep(1000);
                    sb.append("3");
                } catch (Exception e) {
                } finally {
                    lock.unlock();
                    rwLock.destroy();
                }
            }
        };
        Thread t4 = new Thread() {
            @Override
            public void run() {
                DistributedReadWriteLock rwLock = coordinationService.getReadWriteLock("clusterA", "test-lock-1");
                Lock lock = rwLock.readLock();
                try {
                    if (lock.tryLock()) {
                        sb.append("4");
                        synchronized (this) {
                            this.notifyAll();
                        }
                    } else {
                        Thread.sleep(5000);
                    }
                } catch (Exception e) {
                } finally {
                    lock.unlock();
                    rwLock.destroy();
                }
            }
        };

        long start = System.currentTimeMillis();
        t1.start();
        synchronized (t1) {
            t1.wait();
        }
        t2.start();
        synchronized (t2) {
            t2.wait();
        }
        t3.start();
        synchronized (t3) {
            t3.wait();
        }
        t4.start();
        synchronized (t4) {
            t4.wait();
        }
        long end = System.currentTimeMillis();

        assertTrue("No contention for read locks so should not have waited sequentially:  elapsedMillis="
                + (end - start), end - start < 3000);

        DistributedReadWriteLock rwLock = coordinationService.getReadWriteLock("clusterA", "test-lock-1");
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            lock.lock();
            try {
                assertTrue("Unexpected value:  " + sb, sb.toString().length() == 4);
            } finally {
                lock.unlock();
            }
        } finally {
            lock.unlock();
            rwLock.destroy();
        }
    }

    @Test
    public void testWriteLock() throws Exception {

        // use StringBuffer because it is synchronized and thread-safe
        final StringBuffer sb = new StringBuffer();

        // threads to simulate multiple processes
        Thread t1 = new Thread() {
            @Override
            public void run() {
                DistributedReadWriteLock rwLock = coordinationService.getReadWriteLock("clusterA", "test-lock-1");
                Lock lock = rwLock.writeLock();
                lock.lock();
                try {
                    synchronized (this) {
                        this.notifyAll();
                    }
                    sb.append("1");
                } catch (Exception e) {
                } finally {
                    lock.unlock();
                    rwLock.destroy();
                }
            }
        };
        Thread t2 = new Thread() {
            @Override
            public void run() {
                DistributedReadWriteLock rwLock = coordinationService.getReadWriteLock("clusterA", "test-lock-1");
                Lock lock = rwLock.writeLock();
                lock.lock();
                try {
                    synchronized (this) {
                        this.notifyAll();
                    }
                    sb.append("2");
                } catch (Exception e) {
                } finally {
                    lock.unlock();
                    rwLock.destroy();
                }
            }
        };
        Thread t3 = new Thread() {
            @Override
            public void run() {
                DistributedReadWriteLock rwLock = coordinationService.getReadWriteLock("clusterA", "test-lock-1");
                Lock lock = rwLock.writeLock();
                lock.lock();
                try {
                    synchronized (this) {
                        this.notifyAll();
                    }
                    Thread.sleep(1000);
                    sb.append("3");
                } catch (Exception e) {
                } finally {
                    lock.unlock();
                    rwLock.destroy();
                }
            }
        };
        Thread t4 = new Thread() {
            @Override
            public void run() {
                DistributedReadWriteLock rwLock = coordinationService.getReadWriteLock("clusterA", "test-lock-1");
                Lock lock = rwLock.writeLock();
                try {
                    if (lock.tryLock()) {
                        sb.append("4");
                        synchronized (this) {
                            this.notifyAll();
                        }
                    }
                } catch (Exception e) {
                } finally {
                    lock.unlock();
                    rwLock.destroy();
                }
            }
        };

        t1.start();
        synchronized (t1) {
            t1.wait();
        }
        t2.start();
        synchronized (t2) {
            t2.wait();
        }
        t3.start();
        synchronized (t3) {
            t3.wait();
        }
        t4.start();
        synchronized (t4) {
            t4.wait();
        }

        DistributedReadWriteLock rwLock = coordinationService.getReadWriteLock("clusterA", "test-lock-1");
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            lock.lock();
            try {
                assertTrue("Unexpected value:  " + sb, "123".equals(sb.toString()));
            } finally {
                lock.unlock();
            }
        } finally {
            lock.unlock();
            rwLock.destroy();
        }
    }
}