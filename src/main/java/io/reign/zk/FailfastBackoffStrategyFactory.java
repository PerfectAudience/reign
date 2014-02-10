package io.reign.zk;

public class FailfastBackoffStrategyFactory implements BackoffStrategyFactory {

    /** this one can be reused */
    static final FailfastBackoffStrategy FAILFAST_BACKOFF_STRATEGY = new FailfastBackoffStrategy();

    @Override
    public BackoffStrategy get() {
        return FAILFAST_BACKOFF_STRATEGY;
    }
}
