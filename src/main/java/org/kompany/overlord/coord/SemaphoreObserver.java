package org.kompany.overlord.coord;

import org.kompany.overlord.ServiceObserver;

/**
 * 
 * @author ypai
 * 
 */
public abstract class SemaphoreObserver implements ServiceObserver {
    /**
     * @param semaphore
     * @param permitId
     *            the acquired permit that was revoked.
     */
    public void revoked(ZkSemaphore semaphore, String permitId) {

    }

    @Override
    public void stateReset(Object o) {
        // TODO Auto-generated method stub

    }

    @Override
    public void stateUnknown(Object o) {
        // TODO Auto-generated method stub

    }
}
