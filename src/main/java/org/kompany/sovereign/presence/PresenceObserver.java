package org.kompany.sovereign.presence;

/**
 * 
 * @author ypai
 * 
 */
public interface PresenceObserver<T> {

    public void updated(T info);
}
