package org.kompany.overlord.coord;

import java.util.concurrent.locks.Lock;

import org.kompany.overlord.ServiceObserver;

/**
 * 
 * @author ypai
 * 
 */
public abstract class LockObserver implements ServiceObserver {

    /**
     * @param lock
     *            the acquired lock that was revoked.
     */
    public void revoked(Lock lock) {

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
