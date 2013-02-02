package org.kompany.overlord.zookeeper;

/**
 * 
 * @author ypai
 * 
 */
public class FailfastBackoffStrategy implements BackoffStrategy {

    @Override
    public boolean hasNext() {
        return false;
    }

    /**
     */
    @Override
    public Long next() {
        return null;
    }

    /**
     * 
     */
    @Override
    public Long get() {
        return null;
    }
}
