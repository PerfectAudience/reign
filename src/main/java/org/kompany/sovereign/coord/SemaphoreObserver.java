package org.kompany.sovereign.coord;

/**
 * 
 * @author ypai
 * 
 */
public abstract class SemaphoreObserver extends CoordObserver<DistributedSemaphore> {
    /**
     * @param semaphore
     * @param permitId
     *            the acquired permit that was revoked.
     */
    public abstract void revoked(DistributedSemaphore semaphore, String reservationId);

}
