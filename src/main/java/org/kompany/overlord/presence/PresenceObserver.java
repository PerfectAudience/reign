package org.kompany.overlord.presence;

/**
 * 
 * @author ypai
 * 
 * @param <T>
 */
public interface PresenceObserver<T> {

    /**
     * Called when there is a change
     * 
     * @param info
     */
    public void handle(T info);

    /**
     * Called when no longer longer available.
     */
    public void unavailable();
}
