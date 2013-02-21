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
     * @param conf
     *            updated configuration; or null if no longer available
     */
    public void handle(T conf);

}
