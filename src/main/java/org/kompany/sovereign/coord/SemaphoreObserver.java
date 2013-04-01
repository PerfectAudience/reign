package org.kompany.sovereign.coord;

/**
 * 
 * @author ypai
 * 
 */
public interface SemaphoreObserver {
    /**
     * @param semaphore
     * @param permitId
     *            the acquired permit that was revoked.
     */
    public void revoked(DistributedSemaphore semaphore, String reservationId);
}
