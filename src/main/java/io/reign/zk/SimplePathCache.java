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

import io.reign.AbstractZkEventHandler;
import io.reign.ZkClient;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

/**
 * A thread-safe LRU cache of ZooKeeper path data, will auto-update on watched node changes.
 * 
 * @author ypai
 * 
 */
public class SimplePathCache implements PathCache {
    private static final Logger logger = LoggerFactory.getLogger(SimplePathCache.class);

    private final AtomicLong hitCount = new AtomicLong(0);
    private final AtomicLong missCount = new AtomicLong(0);

    private final ConcurrentMap<String, PathCacheEntry> cache;

    // private final ZkClient zkClient;

    // private final ExecutorService executorService;

    private volatile boolean shutdown = false;

    private volatile boolean autoUpdateAll = false;

    public SimplePathCache(int maxSize, int concurrencyLevel) {
        cache = new ConcurrentLinkedHashMap.Builder<String, PathCacheEntry>().maximumWeightedCapacity(maxSize)
                .initialCapacity(maxSize).concurrencyLevel(concurrencyLevel).build();

        // executorService = Executors.newScheduledThreadPool(updaterThreads);

        // this.zkClient = zkClient;
    }

    @Override
    public void init() {

    }

    @Override
    public void destroy() {
        shutdown = true;
        // executorService.shutdown();
    }

    public boolean isAutoUpdateAll() {
        return autoUpdateAll;
    }

    public void setAutoUpdateAll(boolean autoUpdateAll) {
        this.autoUpdateAll = autoUpdateAll;
    }

    /**
     * 
     * @param absolutePath
     * @param ttlMillis
     * @return
     */
    @Override
    public PathCacheEntry get(String absolutePath, int ttlMillis) {
        PathCacheEntry cacheEntry = cache.get(absolutePath);

        // if item is expired, return null
        if (cacheEntry == null || isExpired(cacheEntry, ttlMillis)) {
            missCount.incrementAndGet();
            return null;
        }

        hitCount.incrementAndGet();

        return cacheEntry;
    }

    // @Override
    // public PathCacheEntry get(String absolutePath, int ttlMillis, PathCacheEntryUpdater updater,
    // int updateThresholdMillis) {
    // PathCacheEntry cacheEntry = cache.get(absolutePath);
    //
    // if (cacheEntry == null) {
    // missCount.incrementAndGet();
    // return null;
    // }
    //
    // long timeDiff = System.currentTimeMillis() - cacheEntry.getLastUpdatedTimestampMillis();
    //
    // // if item age is past updateThreshold, then schedule an async refresh to keep cache data fresh
    // if (updateThresholdMillis > 0 && timeDiff > updateThresholdMillis) {
    // executorService.submit(new PathCacheEntryUpdaterRunnable(absolutePath, updater, cache));
    // }
    //
    // // if item is expired, return null
    // if (ttlMillis > 0 && timeDiff > ttlMillis) {
    // missCount.incrementAndGet();
    // return null;
    // }
    //
    // hitCount.incrementAndGet();
    //
    // return cacheEntry;
    // }

    // @Override
    // public PathCacheEntry get(String absolutePath, int ttlMillis, int updateThreshold) {
    // return get(absolutePath, ttlMillis, new DefaultPathCacheEntryUpdater(absolutePath), updateThreshold);
    // }

    @Override
    public PathCacheEntry updateData(String absolutePath, byte[] updatedData) {
        PathCacheEntry pathCacheEntry = cache.get(absolutePath);
        if (pathCacheEntry != null) {
            List<String> children = pathCacheEntry.getChildList();
            Stat stat = pathCacheEntry.getStat();

            /** update cache **/
            PathCacheEntry updatedPathCacheEntry = this.put(absolutePath, stat, updatedData, children);

            return updatedPathCacheEntry;
        }

        return null;
    }

    @Override
    public PathCacheEntry updateChildList(String absolutePath, List<String> updatedChildList) {
        PathCacheEntry pathCacheEntry = cache.get(absolutePath);
        if (pathCacheEntry != null) {
            byte[] data = pathCacheEntry.getData();
            Stat stat = pathCacheEntry.getStat();

            /** update cache **/
            PathCacheEntry updatedPathCacheEntry = this.put(absolutePath, stat, data, updatedChildList);

            return updatedPathCacheEntry;
        }

        return null;
    }

    @Override
    public PathCacheEntry updateStat(String absolutePath, Stat updatedStat) {
        PathCacheEntry pathCacheEntry = cache.get(absolutePath);
        if (pathCacheEntry != null) {
            List<String> childList = pathCacheEntry.getChildList();
            byte[] data = pathCacheEntry.getData();

            /** update cache **/
            PathCacheEntry updatedPathCacheEntry = this.put(absolutePath, updatedStat, data, childList);

            return updatedPathCacheEntry;
        }

        return null;
    }

    /**
     * Get with no TTL.
     * 
     * @param absolutePath
     * @return
     */
    @Override
    public PathCacheEntry get(String absolutePath) {
        PathCacheEntry cacheEntry = cache.get(absolutePath);
        if (cacheEntry == null) {
            missCount.incrementAndGet();
        } else {
            hitCount.incrementAndGet();
        }
        return cacheEntry;
    }

    @Override
    public PathCacheEntry remove(String absolutePath) {
        PathCacheEntry removed = cache.remove(absolutePath);
        if (removed != null) {
            logger.debug("Removed cache entry:  path={}", absolutePath);
        }

        // also remove parent cache entry
        String parentPath = parentPath(absolutePath);
        if (parentPath != null) {
            removed = cache.remove(parentPath);
            if (removed != null) {
                logger.debug("Removed parent cache entry:  path={}", parentPath);
            }
        }

        return removed;
    }

    @Override
    public long getHitCount() {
        return hitCount.get();
    }

    @Override
    public long getMissCount() {
        return missCount.get();
    }

    /**
     * 
     * @param absolutePath
     * @param stat
     * @param bytes
     *            Zookeeper node data
     */
    @Override
    public PathCacheEntry put(String absolutePath, Stat stat, byte[] bytes, List<String> childList) {
        if (childList == null) {
            childList = Collections.EMPTY_LIST;
        }

        return cache.put(absolutePath, new SimplePathCacheEntry(stat, bytes, childList, System.currentTimeMillis()));
    }

    boolean isExpired(PathCacheEntry cacheEntry, int ttlMillis) {
        return (ttlMillis > 0 && System.currentTimeMillis() - cacheEntry.getLastUpdatedTimestampMillis() > ttlMillis);
    }

    String parentPath(String path) {
        if ("/".equals(path)) {
            return null;
        }
        return path.substring(0, path.lastIndexOf("/"));
    }
}
