package io.reign.util;

/**
 * 
 * @author ypai
 * 
 */
public interface PathCacheEntryUpdater {

    /**
     * 
     * @return PathCacheEntry or null if unable to get data for any reason
     */
    public PathCacheEntry get();
}
