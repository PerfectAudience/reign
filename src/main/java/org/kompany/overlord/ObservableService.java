package org.kompany.overlord;

/**
 * Implemented by a service that has observers.
 * 
 * @author ypai
 * 
 */
public interface ObservableService {

    public void signalStateReset(Object o);

    public void signalStateUnknown(Object o);

}
