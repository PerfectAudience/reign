/*
 Copyright 2013 Yen Pai ypai@reign.io

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package io.reign.zk;

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
    public PathCacheEntry get(String absolutePath, int ttlMillis);

    /**
     * Get with no TTL.
     * 
     * @param absolutePath
     * @return
     */
    public PathCacheEntry get(String absolutePath);

    // /**
    // * Behaves the same as get(String absolutePath, int ttl) but will update cache entry in the background if cache
    // * entry age is past updateThreshold.
    // *
    // * @param absolutePath
    // * @param ttl
    // * @param updater
    // * @param updateThreshold
    // * @return
    // */
    // public PathCacheEntry get(String absolutePath, int ttlMillis, PathCacheEntryUpdater updater,
    // int updateThresholdMillis);

    // /**
    // * Uses default updater (updates children, node stat, and node value).
    // *
    // * @param absolutePath
    // * @param ttl
    // * @param updateThreshold
    // * @return
    // */
    // public PathCacheEntry get(String absolutePath, int ttlMillis, int updateThresholdMillis);

    /**
     * 
     * @param absolutePath
     * @param stat
     * @param bytes
     *            Zookeeper node data
     */
    public PathCacheEntry put(String absolutePath, Stat stat, byte[] bytes, List<String> childList);

    /**
     * Update only if path exists in cache.
     * 
     * @param absolutePath
     * @param updatedData
     */
    public PathCacheEntry updateData(String absolutePath, byte[] updatedData);

    /**
     * Update only if path exists in cache.
     * 
     * @param absolutePath
     * @param childList
     */
    public PathCacheEntry updateChildList(String absolutePath, List<String> updatedChildList);

    /**
     * Update only if path exists in cache.
     * 
     * @param absolutePath
     * @param stat
     */
    public PathCacheEntry updateStat(String absolutePath, Stat updatedStat);

    /**
     * 
     * @param absolutePath
     * @return the cache entry removed
     */
    public PathCacheEntry remove(String absolutePath);

    public long getHitCount();

    public long getMissCount();

    public void init();

    public void destroy();
}
