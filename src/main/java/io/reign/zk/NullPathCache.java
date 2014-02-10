package io.reign.zk;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.data.Stat;

/**
 * Null object pattern implementation of PathCache that does not actually cache anything. Used for testing or in
 * situations where path data caching is not desirable.
 * 
 * @author ypai
 * 
 */
public class NullPathCache implements PathCache {

    private final AtomicLong missCount = new AtomicLong(0L);

    @Override
    public PathCacheEntry get(String absolutePath, int ttlMillis) {
        return null;
    }

    @Override
    public PathCacheEntry get(String absolutePath) {
        return null;
    }

    @Override
    public PathCacheEntry put(String absolutePath, Stat stat, byte[] bytes, List<String> children) {
        return null;
    }

    @Override
    public PathCacheEntry remove(String absolutePath) {
        return null;
    }

    @Override
    public PathCacheEntry updateData(String absolutePath, byte[] updatedData) {
        return null;
    }

    @Override
    public PathCacheEntry updateChildList(String absolutePath, List<String> childList) {
        return null;
    }

    @Override
    public PathCacheEntry updateStat(String absolutePath, Stat stat) {
        return null;
    }

    @Override
    public long getHitCount() {
        return 0;
    }

    @Override
    public long getMissCount() {
        return missCount.get();
    }

    @Override
    public void init() {
    }

    @Override
    public void destroy() {
    }

}
