package io.reign.util;

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
public class SimplePathCache extends AbstractZkEventHandler implements PathCache {
    private static final Logger logger = LoggerFactory.getLogger(SimplePathCache.class);

    private final AtomicLong hitCount = new AtomicLong(0);
    private final AtomicLong missCount = new AtomicLong(0);

    private final ConcurrentMap<String, PathCacheEntry> cache;

    private final ZkClient zkClient;

    private final ExecutorService executorService;

    public SimplePathCache(int maxSize, int concurrencyLevel, ZkClient zkClient, int updaterThreads) {
        cache = new ConcurrentLinkedHashMap.Builder<String, PathCacheEntry>().maximumWeightedCapacity(maxSize)
                .initialCapacity(maxSize).concurrencyLevel(concurrencyLevel).build();

        executorService = Executors.newScheduledThreadPool(updaterThreads);

        this.zkClient = zkClient;
    }

    @Override
    public void init() {

    }

    @Override
    public void destroy() {
        executorService.shutdown();
    }

    /**
     * 
     * @param absolutePath
     * @param ttl
     * @return
     */
    @Override
    public PathCacheEntry get(String absolutePath, int ttl) {
        PathCacheEntry cacheEntry = cache.get(absolutePath);

        // if item is expired, return null
        if (System.currentTimeMillis() - cacheEntry.getLastUpdatedTimestampMillis() > ttl) {
            missCount.incrementAndGet();
            return null;
        }

        hitCount.incrementAndGet();

        return cacheEntry;
    }

    @Override
    public PathCacheEntry get(String absolutePath, int ttl, PathCacheEntryUpdater updater, int updateThreshold) {
        PathCacheEntry cacheEntry = cache.get(absolutePath);

        long timeDiff = System.currentTimeMillis() - cacheEntry.getLastUpdatedTimestampMillis();

        // if item age is past updateThreshold, then schedule an async refresh to keep cache data fresh
        if (timeDiff > updateThreshold) {
            executorService.submit(new PathCacheEntryUpdaterRunnable(absolutePath, updater, cache));
        }

        // if item is expired, return null
        if (timeDiff > ttl) {
            missCount.incrementAndGet();
            return null;
        }

        hitCount.incrementAndGet();

        return cacheEntry;
    }

    @Override
    public PathCacheEntry get(String absolutePath, int ttl, int updateThreshold) {
        return get(absolutePath, ttl, new DefaultPathCacheEntryUpdater(absolutePath), updateThreshold);
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
    public void put(String absolutePath, Stat stat, byte[] bytes, List<String> children) {
        if (children == null) {
            children = Collections.EMPTY_LIST;
        }
        cache.put(absolutePath, new PathCacheEntry(stat, bytes, children, System.currentTimeMillis()));
    }

    @Override
    public void nodeChildrenChanged(WatchedEvent event) {
        updateCacheEntry(event);
    }

    @Override
    public void nodeCreated(WatchedEvent event) {
        updateCacheEntry(event);
    }

    @Override
    public void nodeDataChanged(WatchedEvent event) {
        updateCacheEntry(event);
    }

    @Override
    public void nodeDeleted(WatchedEvent event) {
        String path = event.getPath();
        PathCacheEntry removed = cache.remove(path);
        if (removed != null) {
            logger.debug("Change detected:  removed cache entry:  path={}", path);
        }
    }

    void updateCacheEntry(WatchedEvent event) {
        String path = event.getPath();

        try {
            byte[] bytes = null;
            List<String> children = null;
            Stat stat = null;

            /** see what we have in cache **/
            PathCacheEntry cacheEntry = this.get(path);
            if (cacheEntry != null) {
                bytes = cacheEntry.getBytes();
                children = cacheEntry.getChildren();
                stat = cacheEntry.getStat();
            }
            if (stat == null) {
                stat = new Stat();
            }

            /** get update data from zk **/
            if (event.getType() == EventType.NodeDataChanged || event.getType() == EventType.NodeCreated) {
                logger.debug("Change detected:  updating path data:  path={}", path);
                bytes = zkClient.getData(path, true, stat);
            }

            if (event.getType() == EventType.NodeChildrenChanged || event.getType() == EventType.NodeCreated) {
                logger.debug("Change detected:  updating path children:  path={}", path);
                children = zkClient.getChildren(path, true, stat);
            }

            /** update cache **/
            this.put(path, stat, bytes, children);

        } catch (KeeperException e) {
            logger.error(this.getClass().getSimpleName() + ":  error while trying to update cache entry:  " + e
                    + ":  path=" + path, e);
        } catch (InterruptedException e) {
            logger.warn(this.getClass().getSimpleName() + ":  interrupted while trying to update cache entry:  " + e
                    + ":  path=" + path, e);
        }

    }

    /**
     * Wrapper runnable
     * 
     * @author ypai
     * 
     */
    private static class PathCacheEntryUpdaterRunnable implements Runnable {
        private static final AtomicInteger inProgressCount = new AtomicInteger(0);
        private final PathCacheEntryUpdater updater;
        private final String absolutePath;
        private final ConcurrentMap<String, PathCacheEntry> cache;

        PathCacheEntryUpdaterRunnable(String absolutePath, PathCacheEntryUpdater updater,
                ConcurrentMap<String, PathCacheEntry> cache) {
            // increment jobs in progress
            inProgressCount.incrementAndGet();

            this.absolutePath = absolutePath;
            this.updater = updater;
            this.cache = cache;
        }

        @Override
        public void run() {
            PathCacheEntry updatedValue = updater.get();
            if (updatedValue != null) {
                cache.put(absolutePath, updatedValue);
            }

            // decrement jobs in progress
            int inProgressCountValue = inProgressCount.decrementAndGet();
            if (inProgressCountValue > 500) {
                logger.warn("Pending update jobs > 500:  inProgressCount={}", inProgressCountValue);
            } else {
                logger.debug("Pending update jobs:  inProgressCount={}", inProgressCountValue);
            }
        }
    }

    private class DefaultPathCacheEntryUpdater implements PathCacheEntryUpdater {

        private final String absolutePath;

        DefaultPathCacheEntryUpdater(String absolutePath) {
            this.absolutePath = absolutePath;
        }

        @Override
        public PathCacheEntry get() {
            try {
                Stat stat = new Stat();
                byte[] bytes = zkClient.getData(absolutePath, true, stat);
                List<String> children = zkClient.getChildren(absolutePath, true);

                return new PathCacheEntry(stat, bytes, children, System.currentTimeMillis());
            } catch (KeeperException e) {
                logger.error(this.getClass().getSimpleName() + ":  error while trying to update cache entry:  " + e
                        + ":  path=" + absolutePath, e);
            } catch (InterruptedException e) {
                logger.warn(this.getClass().getSimpleName() + ":  interrupted while trying to update cache entry:  "
                        + e + ":  path=" + absolutePath, e);
            }

            return null;
        }

    }

}
