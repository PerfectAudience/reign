package org.kompany.sovereign.conf;

/**
 * 
 * @author ypai
 * 
 */
public interface ConfObserver<T> {

    /**
     * Called when T is updated.
     * 
     * @param data
     */
    public void updated(T data);
}
