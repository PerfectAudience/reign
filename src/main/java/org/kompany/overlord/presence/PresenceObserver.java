package org.kompany.overlord.presence;

import org.kompany.overlord.ServiceObserver;

/**
 * 
 * @author ypai
 * 
 * @param <T>
 */
public abstract class PresenceObserver<T> implements ServiceObserver<T> {

    public abstract void updated(T data);

    @Override
    public void stateReset(Object o) {

    }

    @Override
    public void stateUnknown(Object o) {

    }
}
