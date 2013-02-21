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
     *            updated info; or null if no longer available
     */
    public void handle(T info);

}
