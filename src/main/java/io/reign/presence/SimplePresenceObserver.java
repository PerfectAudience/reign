package io.reign.presence;

import io.reign.ServiceObserver;

/**
 * 
 * @author ypai
 * 
 * @param <T>
 */
public abstract class SimplePresenceObserver<T> implements ServiceObserver, PresenceObserver<T> {

    @Override
    public abstract void updated(T info);

    @Override
    public void stateReset(Object o) {

    }

    @Override
    public void stateUnknown(Object o) {

    }
}
