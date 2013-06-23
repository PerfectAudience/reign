package io.reign.conf;

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
