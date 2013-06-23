package io.reign.coord;

/**
 * 
 * @author ypai
 * 
 */
public abstract class SimpleSemaphoreObserver extends CoordObserver<DistributedSemaphore> {
    /**
     * @param semaphore
     * @param permitId
     *            the acquired permit that was revoked.
     */
    public abstract void revoked(DistributedSemaphore semaphore, String reservationId);

}
