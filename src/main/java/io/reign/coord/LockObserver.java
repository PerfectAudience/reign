package io.reign.coord;

/**
 * 
 * @author ypai
 * 
 */
public interface LockObserver {

    /**
     * @param lock
     *            the acquired lock that was revoked.
     */
    public void revoked(DistributedLock lock, String reservationId);

}
