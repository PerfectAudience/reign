package org.kompany.overlord.coord;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

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
public class ZkLock implements Lock {

    private static final Logger logger = LoggerFactory.getLogger(ZkLock.class);

    private final ZkLockManager zkLockManager;
    private final String ownerId;
    private final PathContext pathContext;
    private final String relativeLockPath;
    private final ReservationType lockType;
    private final List<ACL> aclList;

    private String acquiredLockPath;

    public ZkLock(ZkLockManager zkLockManager, String ownerId, PathContext pathContext, String relativeLockPath,
            ReservationType lockType, List<ACL> aclList) {
        super();
        this.zkLockManager = zkLockManager;
        this.ownerId = ownerId;
        this.pathContext = pathContext;
        this.lockType = lockType;
        this.relativeLockPath = relativeLockPath;
        this.aclList = aclList;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.locks.Lock#lock()
     */
    @Override
    public void lock() {
        try {
            acquiredLockPath = zkLockManager.acquire(ownerId, pathContext, relativeLockPath, lockType, aclList, -1,
                    false);
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
    public void lockInterruptibly() throws InterruptedException {
        acquiredLockPath = zkLockManager.acquire(ownerId, pathContext, relativeLockPath, lockType, aclList, -1, true);

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
    public boolean tryLock() {
        try {
            acquiredLockPath = zkLockManager.acquire(ownerId, pathContext, relativeLockPath, lockType, aclList, 0,
                    false);
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
    public boolean tryLock(long wait, TimeUnit timeUnit) throws InterruptedException {
        // convert wait to millis
        long timeWaitMillis = TimeUnitUtils.toMillis(wait, timeUnit);

        // attempt to acquire lock
        acquiredLockPath = zkLockManager.acquire(ownerId, pathContext, relativeLockPath, lockType, aclList,
                timeWaitMillis, true);

        return acquiredLockPath != null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.locks.Lock#unlock()
     */
    @Override
    public void unlock() {
        zkLockManager.relinquish(acquiredLockPath);
    }

}
