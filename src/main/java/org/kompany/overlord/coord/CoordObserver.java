package org.kompany.overlord.coord;

import org.kompany.overlord.ServiceObserver;

/**
 * 
 * @author ypai
 * 
 * @param <T>
 */
public abstract class CoordObserver<T> implements ServiceObserver<T> {
    @Override
    public void stateReset(Object o) {
        // TODO Auto-generated method stub

    }

    @Override
    public void stateUnknown(Object o) {
        // TODO Auto-generated method stub

    }
}
