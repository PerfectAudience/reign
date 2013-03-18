package org.kompany.sovereign.util;

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
     * 
     * @param absolutePath
     * @param stat
     * @param bytes
     *            Zookeeper node data
     */
    public void put(String absolutePath, Stat stat, byte[] bytes, List<String> children);

}
