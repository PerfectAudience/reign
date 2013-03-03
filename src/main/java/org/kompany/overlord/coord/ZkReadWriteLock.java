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

    public ZkReadWriteLock(ZkReservationManager zkLockManager, String ownerId, PathContext pathContext, String clusterId,
            String lockName, List<ACL> aclList) {
        super();

        writeLock = new ZkLock(zkLockManager, ownerId, PathContext.USER, clusterId, lockName,
                ReservationType.LOCK_EXCLUSIVE, aclList);
        readLock = new ZkLock(zkLockManager, ownerId, PathContext.USER, clusterId, lockName,
                ReservationType.LOCK_SHARED, aclList);
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
