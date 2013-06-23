package io.reign.presence;

/**
 * 
 * @author ypai
 * 
 */
public interface PresenceObserver<T> {

    public void updated(T info);
}
