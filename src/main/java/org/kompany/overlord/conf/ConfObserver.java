package org.kompany.overlord.conf;

/**
 * 
 * @author ypai
 * 
 */
public interface ConfObserver<T> {

    /**
     * Called when there is a change
     * 
     * @param info
     */
    public void handle(T conf);

    /**
     * Called when no longer longer available.
     */
    public void unavailable();
}
