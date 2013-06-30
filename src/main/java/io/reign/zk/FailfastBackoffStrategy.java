package io.reign.zk;

/**
 * 
 * @author ypai
 * 
 */
public class FailfastBackoffStrategy implements BackoffStrategy {

    /** this one can be reused */
    public static final FailfastBackoffStrategy FAILFAST_BACKOFF_STRATEGY = new FailfastBackoffStrategy();

    private FailfastBackoffStrategy() {

    }

    @Override
    public boolean hasNext() {
        return false;
    }

    /**
     */
    @Override
    public Integer next() {
        return null;
    }

    /**
     * 
     */
    @Override
    public Integer get() {
        return null;
    }
}
