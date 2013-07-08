/*
 Copyright 2013 Yen Pai ypai@reign.io

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package io.reign.coord;

import io.reign.util.TimeUnitUtil;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.apache.zookeeper.data.ACL;
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
    private final String entityPath;
    // private final PathContext pathContext;
    // private final String clusterId;
    // private final String lockName;
    private final ReservationType reservationType;
    private final List<ACL> aclList;

    private String acquiredLockPath;

    public ZkLock(ZkReservationManager zkReservationManager, String ownerId, String entityPath,
            ReservationType reservationType, List<ACL> aclList) {
        super();
        this.zkReservationManager = zkReservationManager;
        this.ownerId = ownerId;
        this.entityPath = entityPath;
        // this.pathContext = pathContext;
        this.reservationType = reservationType;
        // this.clusterId = clusterId;
        // this.lockName = lockName;
        this.aclList = aclList;
    }

    @Override
    public void destroy() {
        logger.trace("destroy() called");
        zkReservationManager.destroyLock(entityPath, reservationType, this);
    }

    @Override
    public boolean isRevoked() {
        return this.acquiredLockPath == null;
    }

    @Override
    public void revoke(String reservationId) {
        if (reservationId != null && reservationId.equals(acquiredLockPath)) {
            acquiredLockPath = null;
        }
    }

    @Override
    public String getLockId() {
        return this.acquiredLockPath;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.locks.Lock#lock()
     */
    @Override
    public void lock() {
        try {
            if (acquiredLockPath == null) {
                acquiredLockPath = zkReservationManager.acquire(ownerId, entityPath, reservationType, aclList, -1,
                        false);
            } else {
                synchronized (this) {
                    this.wait();
                }
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
    public void lockInterruptibly() throws InterruptedException {
        if (acquiredLockPath == null) {
            acquiredLockPath = zkReservationManager.acquire(ownerId, entityPath, reservationType, aclList, -1, true);
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
    public boolean tryLock() {
        try {
            if (acquiredLockPath == null) {
                acquiredLockPath = zkReservationManager
                        .acquire(ownerId, entityPath, reservationType, aclList, 0, false);
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
    public boolean tryLock(long wait, TimeUnit timeUnit) throws InterruptedException {
        if (acquiredLockPath == null) {
            // convert wait to millis
            long timeWaitMillis = TimeUnitUtil.toMillis(wait, timeUnit);

            // attempt to acquire lock
            acquiredLockPath = zkReservationManager.acquire(ownerId, entityPath, reservationType, aclList,
                    timeWaitMillis, true);
        }

        return acquiredLockPath != null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.locks.Lock#unlock()
     */
    @Override
    public void unlock() {
        String tmpAcquiredLockPath = acquiredLockPath;
        acquiredLockPath = null;
        if (!zkReservationManager.relinquish(tmpAcquiredLockPath)) {
            acquiredLockPath = tmpAcquiredLockPath;
        } else {
            synchronized (this) {
                this.notifyAll();
            }
        }
    }

}
