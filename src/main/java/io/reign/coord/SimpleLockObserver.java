package io.reign.coord;

/**
 * 
 * @author ypai
 * 
 */
public abstract class SimpleLockObserver extends CoordObserver<DistributedLock> {

    /**
     * @param lock
     *            the acquired lock that was revoked.
     */
    public abstract void revoked(DistributedLock lock, String reservationId);

}
