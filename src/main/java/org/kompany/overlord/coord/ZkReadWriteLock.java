package org.kompany.overlord.coord;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.zookeeper.data.ACL;
import org.kompany.overlord.PathContext;

/**
 * 
 * @author ypai
 * 
 */
public class ZkReadWriteLock implements ReadWriteLock {

    private final Lock readLock;
    private final Lock writeLock;

    public ZkReadWriteLock(ZkLockManager zkLockManager, String ownerId, PathContext pathContext,
            String relativeLockPath, List<ACL> aclList) {
        super();

        writeLock = new ZkLock(zkLockManager, ownerId, PathContext.USER, relativeLockPath, ReservationType.LOCK_EXCLUSIVE, aclList);
        readLock = new ZkLock(zkLockManager, ownerId, PathContext.USER, relativeLockPath, ReservationType.LOCK_SHARED, aclList);
    }

    @Override
    public Lock readLock() {
        return readLock;
    }

    @Override
    public Lock writeLock() {
        return writeLock;
    }

}
