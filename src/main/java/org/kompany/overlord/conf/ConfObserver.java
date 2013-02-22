package org.kompany.overlord.conf;

import org.kompany.overlord.ServiceObserver;

/**
 * 
 * @author ypai
 * 
 */
public abstract class ConfObserver<T> implements ServiceObserver<T> {

    public abstract void updated(T data);

    @Override
    public void stateReset(Object o) {

    }

    @Override
    public void stateUnknown(Object o) {

    }

}
