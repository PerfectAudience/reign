package org.kompany.sovereign.conf;

import org.kompany.sovereign.ServiceObserver;

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
