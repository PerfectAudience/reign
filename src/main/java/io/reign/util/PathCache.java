package io.reign.util;

import java.util.List;

import org.apache.zookeeper.data.Stat;

/**
 * Interface for path cache implementations.
 * 
 * @author ypai
 * 
 */
public interface PathCache {

    /**
     * 
     * @param absolutePath
     * @param ttl
     * @return
     */
    public PathCacheEntry get(String absolutePath, int ttl);

    /**
     * Get with no TTL.
     * 
     * @param absolutePath
     * @return
     */
    public PathCacheEntry get(String absolutePath);

    /**
     * Behaves the same as get(String absolutePath, int ttl) but will update cache entry in the background if cache
     * entry age is past updateThreshold.
     * 
     * @param absolutePath
     * @param ttl
     * @param updater
     * @param updateThreshold
     * @return
     */
    public PathCacheEntry get(String absolutePath, int ttl, PathCacheEntryUpdater updater, int updateThreshold);

    /**
     * Uses default updater (updates children, node stat, and node value).
     * 
     * @param absolutePath
     * @param ttl
     * @param updateThreshold
     * @return
     */
    public PathCacheEntry get(String absolutePath, int ttl, int updateThreshold);

    /**
     * 
     * @param absolutePath
     * @param stat
     * @param bytes
     *            Zookeeper node data
     */
    public void put(String absolutePath, Stat stat, byte[] bytes, List<String> children);

    public long getHitCount();

    public long getMissCount();

    public void init();

    public void destroy();
}
