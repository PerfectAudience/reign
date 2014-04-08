package io.reign.coord;

import static org.junit.Assert.assertTrue;
import io.reign.MasterTestSuite;

import org.junit.Before;
import org.junit.Test;

public class ZkReentrantLockTest {

    private CoordinationService coordinationService;

    @Before
    public void setUp() throws Exception {

        coordinationService = MasterTestSuite.getReign().getService("coord");

    }

    @Test
    public void testBasic() throws Exception {

        // use StringBuffer because it is synchronized and thread-safe
        final StringBuffer sb = new StringBuffer();

        // threads to simulate multiple processes
        Thread t1 = new Thread() {
            @Override
            public void run() {
                DistributedReentrantLock lock = coordinationService.getReentrantLock("clusterA", "test-lock-1");
                lock.lock();
                try {
                    synchronized (this) {
                        this.notifyAll();
                    }
                    sb.append("1");
                } catch (Exception e) {
                } finally {
                    lock.unlock();
                    lock.destroy();
                }
            }
        };
        Thread t2 = new Thread() {
            @Override
            public void run() {
                DistributedReentrantLock lock = coordinationService.getReentrantLock("clusterA", "test-lock-1");
                lock.lock();
                try {
                    synchronized (this) {
                        this.notifyAll();
                    }
                    sb.append("2");
                } catch (Exception e) {
                } finally {
                    lock.unlock();
                    lock.destroy();
                }
            }
        };
        Thread t3 = new Thread() {
            @Override
            public void run() {
                DistributedReentrantLock lock = coordinationService.getReentrantLock("clusterA", "test-lock-1");
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
                    lock.destroy();
                }
            }
        };
        Thread t4 = new Thread() {
            @Override
            public void run() {
                DistributedReentrantLock lock = coordinationService.getReentrantLock("clusterA", "test-lock-1");
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
                    lock.destroy();
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

        DistributedReentrantLock lock = coordinationService.getReentrantLock("clusterA", "test-lock-1");
        lock.lock();
        try {
            lock.lock();
            try {
                assertTrue("Unexpected value:  " + lock.getHoldCount(), lock.getHoldCount() == 2);
                assertTrue("Unexpected value:  " + sb, "123".equals(sb.toString()));
            } finally {
                lock.unlock();
            }
        } finally {
            lock.unlock();
            assertTrue("Unexpected value:  " + lock.getHoldCount(), lock.getHoldCount() == 0);
            lock.destroy();
        }
    }
}