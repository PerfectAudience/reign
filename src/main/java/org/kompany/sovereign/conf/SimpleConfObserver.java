package org.kompany.sovereign.conf;

import org.kompany.sovereign.ServiceObserver;

/**
 * 
 * @author ypai
 * 
 */
public abstract class SimpleConfObserver<T> implements ServiceObserver, ConfObserver<T> {

    @Override
    public abstract void updated(T data);

    @Override
    public void stateReset(Object o) {

    }

    @Override
    public void stateUnknown(Object o) {

    }

}
