package org.kompany.overlord;

/**
 * 
 * @author ypai
 * 
 */
public interface ServiceObserver<T> {
    /**
     * Called when there is a change
     * 
     * @param data
     *            updated data; or null if no longer available
     */
    public void handle(T data);
}
