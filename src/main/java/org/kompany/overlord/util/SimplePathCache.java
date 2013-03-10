package org.kompany.overlord.util;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.kompany.overlord.AbstractZkEventHandler;
import org.kompany.overlord.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

/**
 * A thread-safe LRU cache of ZooKeeper path data, will auto-update on watched
 * node changes.
 * 
 * @author ypai
 * 
 */
public class SimplePathCache extends AbstractZkEventHandler implements PathCache {
    private static final Logger logger = LoggerFactory.getLogger(SimplePathCache.class);

    private final ConcurrentMap<String, PathCacheEntry> cache;

    private final ZkClient zkClient;

    public SimplePathCache(int maxSize, int concurrencyLevel, ZkClient zkClient) {
        cache = new ConcurrentLinkedHashMap.Builder<String, PathCacheEntry>().maximumWeightedCapacity(maxSize)
                .initialCapacity(maxSize).concurrencyLevel(concurrencyLevel).build();

        this.zkClient = zkClient;
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
    @Override
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
    @Override
    public void put(String absolutePath, Stat stat, byte[] bytes, List<String> children) {
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
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    // @Override
    // public void process(WatchedEvent event) {
    // // log if DEBUG
    // if (logger.isDebugEnabled()) {
    // logger.debug("***** Received ZooKeeper Event:  {}",
    // ReflectionToStringBuilder.toString(event, ToStringStyle.DEFAULT_STYLE));
    //
    // }
    //
    // /** process events **/
    // String path = event.getPath();
    // boolean updateRequired = false;
    // switch (event.getType()) {
    // case NodeChildrenChanged:
    // updateRequired = true;
    // break;
    // case NodeCreated:
    // // don't do anything until it is in the cache through other usages
    // break;
    // case NodeDataChanged:
    // updateRequired = true;
    // break;
    // case NodeDeleted:
    // PathCacheEntry removed = cache.remove(path);
    // if (removed != null) {
    // logger.info("Change detected:  removed cache entry:  path={}", path);
    // }
    // break;
    // case None:
    // default:
    // logger.warn("Unhandled event type:  eventType=" + event.getType() +
    // "; eventState=" + event.getState());
    // }
    //
    // /** refresh removed data **/
    // if (updateRequired) {
    // // repopulate from ZK
    // try {
    // byte[] bytes = null;
    // List<String> children = null;
    //
    // // see what we have in cache
    // PathCacheEntry cacheEntry = this.get(path);
    // if (cacheEntry != null) {
    // bytes = cacheEntry.getBytes();
    // children = cacheEntry.getChildren();
    // }
    //
    // // load data from ZK
    // Stat stat = new Stat();
    //
    // if (event.getType() == EventType.NodeDataChanged) {
    // logger.info("Change detected:  updating path data:  path={}", path);
    // bytes = zkClient.getData(path, true, stat);
    // }
    //
    // if (event.getType() == EventType.NodeChildrenChanged) {
    // logger.info("Change detected:  updating path children:  path={}", path);
    // children = zkClient.getChildren(path, true, stat);
    // }
    //
    // // update cache
    // this.put(path, stat, bytes, children);
    //
    // } catch (KeeperException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // } catch (InterruptedException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }
    //
    // }
    // }

}
