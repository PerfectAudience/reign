package org.kompany.overlord.util;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.kompany.overlord.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

/**
 * A thread-safe LRU cache of ZooKeeper path data, will auto-update on watched node changes.
 * 
 * @author ypai
 * 
 */
public class PathCache implements Watcher {
    private static final Logger logger = LoggerFactory.getLogger(PathCache.class);

    private ConcurrentMap<String, PathCacheEntry> cache;

    private ZkClient zkClient;

    public PathCache(int maxSize, int concurrencyLevel, ZkClient zkClient) {
        cache = new ConcurrentLinkedHashMap.Builder<String, PathCacheEntry>().maximumWeightedCapacity(10000)
                .initialCapacity(maxSize).concurrencyLevel(concurrencyLevel).build();

        this.zkClient = zkClient;

        // zkClient.register(this);
    }

    /**
     * 
     * @param absolutePath
     * @param ttl
     * @return
     */
    public PathCacheEntry get(String absolutePath, int ttl) {
        PathCacheEntry cacheEntry = cache.get(absolutePath);
        if (cacheEntry.getLastUpdatedTimestampMillis() + ttl < System.currentTimeMillis()) {
            return null;
        }
        return cacheEntry;
    }

    /**
     * Get with no TTL.
     * 
     * @param absolutePath
     * @return
     */
    public PathCacheEntry get(String absolutePath) {
        return cache.get(absolutePath);

    }

    /**
     * 
     * @param absolutePath
     * @param stat
     * @param bytes
     *            Zookeeper node data
     */
    public void put(String absolutePath, Stat stat, byte[] bytes, List<String> children) {
        cache.put(absolutePath, new PathCacheEntry(stat, bytes, children, System.currentTimeMillis()));
    }

    @Override
    public void process(WatchedEvent event) {
        // log if DEBUG
        if (logger.isDebugEnabled()) {
            logger.debug("***** Received ZooKeeper Event:  {}",
                    ReflectionToStringBuilder.toString(event, ToStringStyle.DEFAULT_STYLE));

        }

        /** process events **/
        String path = event.getPath();
        boolean updateRequired = false;
        switch (event.getType()) {
        case NodeChildrenChanged:
            updateRequired = true;
            break;
        case NodeCreated:
            // don't do anything until it is in the cache through other usages
            break;
        case NodeDataChanged:
            updateRequired = true;
            break;
        case NodeDeleted:
            PathCacheEntry removed = cache.remove(path);
            if (removed != null) {
                logger.info("Change detected:  removed cache entry:  path={}", path);
            }
            break;
        case None:
        default:
            logger.warn("Unhandled event type:  eventType=" + event.getType() + "; eventState=" + event.getState());
        }

        /** refresh removed data **/
        if (updateRequired) {
            // repopulate from ZK
            try {
                byte[] bytes = null;
                List<String> children = null;

                // see what we have in cache
                PathCacheEntry cacheEntry = this.get(path);
                if (cacheEntry != null) {
                    bytes = cacheEntry.getBytes();
                    children = cacheEntry.getChildren();
                }

                // load data from ZK
                Stat stat = new Stat();

                if (event.getType() == EventType.NodeDataChanged) {
                    logger.info("Change detected:  updating path data:  path={}", path);
                    bytes = zkClient.getData(path, true, stat);
                }

                if (event.getType() == EventType.NodeChildrenChanged) {
                    logger.info("Change detected:  updating path children:  path={}", path);
                    children = zkClient.getChildren(path, false, stat);
                }

                // update cache
                this.put(path, stat, bytes, children);

            } catch (KeeperException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }
    }

    public static class PathCacheEntry {
        private Stat stat;
        private byte[] bytes;
        private List<String> children;
        private long lastUpdatedTimestampMillis;

        public PathCacheEntry(Stat stat, byte[] bytes, List<String> children, long lastUpdateTimestampMillis) {
            this.stat = stat;
            this.bytes = bytes;
            this.children = children;
            this.lastUpdatedTimestampMillis = lastUpdateTimestampMillis;
        }

        public long getLastUpdatedTimestampMillis() {
            return lastUpdatedTimestampMillis;
        }

        public void setLastUpdatedTimestampMillis(long lastUpdatedTimestampMillis) {
            this.lastUpdatedTimestampMillis = lastUpdatedTimestampMillis;
        }

        public Stat getStat() {
            return stat;
        }

        public void setStat(Stat stat) {
            this.stat = stat;
        }

        public byte[] getBytes() {
            return bytes;
        }

        public void setBytes(byte[] bytes) {
            this.bytes = bytes;
        }

        public List<String> getChildren() {
            return children;
        }

        public void setChildren(List<String> children) {
            this.children = children;
        }

    }
}
