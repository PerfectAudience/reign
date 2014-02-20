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

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZkClient implementation with underlying path cache.
 * 
 * Requests without a watch request (watch flag set to true or Watcher argument) will consult the path cache first.
 * 
 * Read requests will result in the requested path being tracked by the cache (updates by other nodes will be picked up
 * via ZooKeeper watches).
 * 
 * Write requests will update path cache items if the ZooKeeper operation is successful and the path is already being
 * tracked by the cache.
 * 
 * @author ypai
 * 
 */
public class ResilientZkClientWithCache extends AbstractZkEventHandler implements ZkClient {

    private static final Logger logger = LoggerFactory.getLogger(ResilientZkClientWithCache.class);

    private final ZkClient zkClient;

    private final PathCache pathCache;

    public ResilientZkClientWithCache(String zkConnectString, int zkSessionTimeout, PathCache pathCache)
            throws IOException {
        zkClient = new ResilientZkClient(zkConnectString, zkSessionTimeout);

        this.pathCache = pathCache;
    }

    @Override
    public void register(Watcher watcher) {
        zkClient.register(watcher);
    }

    @Override
    public void close() {
        zkClient.close();
    }

    @Override
    public Stat exists(String path, boolean watch) throws KeeperException, InterruptedException {
        if (!watch) {
            PathCacheEntry pathCacheEntry = pathCache.get(path);
            if (pathCacheEntry != null) {
                return pathCacheEntry.getStat();
            }
        }

        Stat stat = zkClient.exists(path, watch);

        pathCache.updateStat(path, stat);
        if (stat == null) {
            pathCache.remove(path);
        }

        return stat;
    }

    @Override
    public Stat exists(String path, Watcher watcher) throws KeeperException, InterruptedException {
        if (watcher == null) {
            PathCacheEntry pathCacheEntry = pathCache.get(path);
            if (pathCacheEntry != null) {
                return pathCacheEntry.getStat();
            }
        }

        Stat stat = zkClient.exists(path, watcher);

        pathCache.updateStat(path, stat);
        if (stat == null) {
            pathCache.remove(path);
        }

        return stat;
    }

    @Override
    public List<String> getChildren(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
        // if no watch is to be set, try to get from path cache
        if (!watch) {
            PathCacheEntry pathCacheEntry = pathCache.get(path);
            if (pathCacheEntry != null) {
                copyStat(pathCacheEntry.getStat(), stat);
                return pathCacheEntry.getChildList();
            }
        }

        List<String> childList = zkClient.getChildren(path, true);
        byte[] data = zkClient.getData(path, true, stat);

        pathCache.put(path, stat, data, childList);

        return childList;
    }

    @Override
    public List<String> getChildren(String path, Watcher watcher) throws KeeperException, InterruptedException {
        // if no watch is to be set, try to get from path cache
        if (watcher == null) {
            PathCacheEntry pathCacheEntry = pathCache.get(path);
            if (pathCacheEntry != null) {
                return pathCacheEntry.getChildList();
            }
        }

        List<String> childList = zkClient.getChildren(path, watcher);

        Stat stat = new Stat();
        byte[] data = zkClient.getData(path, true, stat);

        pathCache.put(path, stat, data, childList);

        return childList;
    }

    @Override
    public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException {
        // if no watch is to be set, try to get from path cache
        if (!watch) {
            PathCacheEntry pathCacheEntry = pathCache.get(path);
            if (pathCacheEntry != null) {
                return pathCacheEntry.getChildList();
            }
        }

        List<String> childList = zkClient.getChildren(path, watch);

        Stat stat = new Stat();
        byte[] data = zkClient.getData(path, true, stat);

        pathCache.put(path, stat, data, childList);

        return childList;
    }

    @Override
    public Stat setData(String path, byte[] data, int version) throws KeeperException, InterruptedException {
        Stat stat = zkClient.setData(path, data, version);

        pathCache.updateData(path, data);
        pathCache.updateStat(path, stat);

        return stat;
    }

    @Override
    public byte[] getData(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
        // if no watch is to be set, try to get from path cache
        if (!watch) {
            PathCacheEntry pathCacheEntry = pathCache.get(path);
            if (pathCacheEntry != null) {
                copyStat(pathCacheEntry.getStat(), stat);
                return pathCacheEntry.getData();
            }
        }

        byte[] data = zkClient.getData(path, watch, stat);
        List<String> childList = zkClient.getChildren(path, true);

        pathCache.put(path, stat, data, childList);

        return data;
    }

    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException,
            InterruptedException {
        String created = zkClient.create(path, data, acl, createMode);

        return created;
    }

    @Override
    public void delete(String path, int version) throws InterruptedException, KeeperException {
        zkClient.delete(path, version);

        pathCache.remove(path);
    }

    @Override
    public void sync(String path, VoidCallback cb, Object ctx) {
        zkClient.sync(path, cb, ctx);
    }

    @Override
    public void nodeChildrenChanged(WatchedEvent event) {
        handleNodeUpdate(event);
    }

    @Override
    public void nodeCreated(WatchedEvent event) {
        handleNodeUpdate(event);
    }

    @Override
    public void nodeDataChanged(WatchedEvent event) {
        handleNodeUpdate(event);
    }

    @Override
    public void nodeDeleted(WatchedEvent event) {
        String path = event.getPath();
        PathCacheEntry removed = pathCache.remove(path);
        if (removed != null) {
            logger.debug("Change detected:  removed cache entry:  path={}", path);
        }
    }

    void handleNodeUpdate(WatchedEvent event) {
        String path = event.getPath();

        logger.info("Updating cache entry:  path={}; eventType={}", path, event.getType());

        try {
            byte[] bytes = null;
            List<String> children = null;
            Stat stat = null;

            /** see what we have in cache **/
            PathCacheEntry cacheEntry = pathCache.get(path);
            if (cacheEntry != null) {
                bytes = cacheEntry.getData();
                children = cacheEntry.getChildList();
                stat = cacheEntry.getStat();
            }

            if (stat == null) {
                stat = new Stat();
            }

            /** get update data from zk **/
            if (event.getType() == EventType.NodeDataChanged || event.getType() == EventType.NodeCreated) {
                logger.debug("Change detected:  updating path data:  path={}", path);
                bytes = zkClient.getData(path, true, stat);

                if (cacheEntry == null) {
                    children = zkClient.getChildren(path, true);
                }
            }

            if (event.getType() == EventType.NodeChildrenChanged || event.getType() == EventType.NodeCreated) {
                logger.debug("Change detected:  updating path children:  path={}", path);
                children = zkClient.getChildren(path, true);

                if (cacheEntry == null) {
                    bytes = zkClient.getData(path, true, stat);
                }
            }

            /** update cache **/
            // watches have already been set above so no need to call pathCache.put() wrapper method in this class
            pathCache.put(path, stat, bytes, children);

        } catch (KeeperException e) {
            logger.error(this.getClass().getSimpleName() + ":  error while trying to update cache entry:  " + e
                    + ":  path=" + path, e);
        } catch (InterruptedException e) {
            logger.warn(this.getClass().getSimpleName() + ":  interrupted while trying to update cache entry:  " + e
                    + ":  path=" + path, e);
        }

    }

    /**
     * Copy data from src to target and return target.
     * 
     * @param src
     * @param target
     * @return
     */
    Stat copyStat(Stat src, Stat target) {
        target.setAversion(src.getAversion());
        target.setCtime(src.getCtime());
        target.setCversion(src.getCversion());
        target.setCzxid(src.getCzxid());
        target.setDataLength(src.getDataLength());
        target.setEphemeralOwner(src.getEphemeralOwner());
        target.setMtime(src.getMtime());
        target.setMzxid(src.getMzxid());
        target.setNumChildren(src.getNumChildren());
        target.setPzxid(src.getPzxid());
        target.setVersion(src.getVersion());

        return target;
    }

}
