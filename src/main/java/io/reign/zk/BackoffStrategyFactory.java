package io.reign.zk;

/**
 * 
 * @author ypai
 * 
 */
public interface BackoffStrategyFactory {

    public BackoffStrategy get();
}
