package io.reign.zookeeper;

/**
 * 
 * @author ypai
 * 
 */
public interface BackoffStrategyFactory {

    public BackoffStrategy get();
}
