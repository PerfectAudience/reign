package org.kompany.overlord.coord;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.apache.zookeeper.data.ACL;
import org.kompany.overlord.PathContext;
import org.kompany.overlord.util.TimeUnitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class ZkLock implements DistributedLock {

    private static final Logger logger = LoggerFactory.getLogger(ZkLock.class);

    private final ZkReservationManager zkReservationManager;
    private final String ownerId;
    private final PathContext pathContext;
    private final String clusterId;
    private final String lockName;
    private final ReservationType lockType;
    private final List<ACL> aclList;

    private String acquiredLockPath;

    public ZkLock(ZkReservationManager zkReservationManager, String ownerId, PathContext pathContext, String clusterId,
            String lockName, ReservationType lockType, List<ACL> aclList) {
        super();
        this.zkReservationManager = zkReservationManager;
        this.ownerId = ownerId;
        this.pathContext = pathContext;
        this.lockType = lockType;
        this.clusterId = clusterId;
        this.lockName = lockName;
        this.aclList = aclList;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.locks.Lock#lock()
     */
    @Override
    public synchronized void lock() {
        try {
            if (acquiredLockPath == null) {
                acquiredLockPath = zkReservationManager.acquire(ownerId, pathContext, clusterId, lockName, lockType,
                        aclList, -1, false);
            } else {
                this.wait();
            }
        } catch (InterruptedException e) {
            logger.warn("Interrupted in lock():  should not happen:  " + e, e);
        }

    }

    /*
     * 
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.locks.Lock#lockInterruptibly()
     */
    @Override
    public synchronized void lockInterruptibly() throws InterruptedException {
        if (acquiredLockPath == null) {
            acquiredLockPath = zkReservationManager.acquire(ownerId, pathContext, clusterId, lockName, lockType,
                    aclList, -1, true);
        } else {
            this.wait();
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.locks.Lock#newCondition()
     */
    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.locks.Lock#tryLock()
     */
    @Override
    public synchronized boolean tryLock() {
        try {
            if (acquiredLockPath == null) {
                acquiredLockPath = zkReservationManager.acquire(ownerId, pathContext, clusterId, lockName, lockType,
                        aclList, 0, false);
            }
        } catch (InterruptedException e) {
            logger.warn("Interrupted in lock():  should not happen:  " + e, e);
        }
        return acquiredLockPath != null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.locks.Lock#tryLock(long,
     * java.util.concurrent.TimeUnit)
     */
    @Override
    public synchronized boolean tryLock(long wait, TimeUnit timeUnit) throws InterruptedException {
        if (acquiredLockPath == null) {
            // convert wait to millis
            long timeWaitMillis = TimeUnitUtils.toMillis(wait, timeUnit);

            // attempt to acquire lock
            acquiredLockPath = zkReservationManager.acquire(ownerId, pathContext, clusterId, lockName, lockType,
                    aclList, timeWaitMillis, true);
        }

        return acquiredLockPath != null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.locks.Lock#unlock()
     */
    @Override
    public synchronized void unlock() {
        zkReservationManager.relinquish(acquiredLockPath);
        acquiredLockPath = null;
        this.notifyAll();
    }

}
