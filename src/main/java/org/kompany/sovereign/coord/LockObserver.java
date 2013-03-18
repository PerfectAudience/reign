package org.kompany.sovereign.coord;

/**
 * 
 * @author ypai
 * 
 */
public abstract class LockObserver extends CoordObserver<DistributedLock> {

    /**
     * @param lock
     *            the acquired lock that was revoked.
     */
    public abstract void revoked(DistributedLock lock, String reservationId);

}
