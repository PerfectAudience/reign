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

package io.reign.data;

import io.reign.DataSerializer;
import io.reign.PathScheme;
import io.reign.ReignContext;
import io.reign.ZkClient;
import io.reign.coord.DistributedReadWriteLock;
import io.reign.util.PathCache;
import io.reign.util.PathCacheEntry;

import java.util.List;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 * @param <K>
 */
public class ZkMultiMapData<K> implements MultiMapData<K> {

    private static final Logger logger = LoggerFactory.getLogger(ZkMultiMapData.class);

    private final DistributedReadWriteLock readWriteLock;

    private final String absoluteBasePath;

    private final PathScheme pathScheme;

    private final List<ACL> aclList;

    private final ZkClientMultiDataUtil zkClientMultiDataUtil;

    private final ZkClient zkClient;

    private final PathCache pathCache;

    /**
     * 
     * @param basePath
     * @param readWriteLock
     *            for inter-process safety; can be null
     */
    public ZkMultiMapData(String absoluteBasePath, DistributedReadWriteLock readWriteLock, List<ACL> aclList,
            TranscodingScheme transcodingScheme, ReignContext context) {

        this.absoluteBasePath = absoluteBasePath;

        this.pathScheme = context.getPathScheme();

        this.zkClient = context.getZkClient();

        this.pathCache = context.getPathCache();

        this.aclList = aclList;
        this.readWriteLock = readWriteLock;

        this.zkClientMultiDataUtil = new ZkClientMultiDataUtil(context.getZkClient(), context.getPathScheme(),
                context.getPathCache(), transcodingScheme);

    }

    @Override
    public synchronized void destroy() {
        if (readWriteLock != null) {
            readWriteLock.destroy();
        }
    }

    @Override
    public synchronized <V> void put(K key, String index, V value) {
        throwExceptionIfKeyIsInvalid(key);
        zkClientMultiDataUtil.lockForWrite(readWriteLock);
        try {
            zkClientMultiDataUtil.writeData(absoluteKeyPath(key), index, value, aclList);
        } finally {
            zkClientMultiDataUtil.unlockForWrite(readWriteLock);
        }
    }

    @Override
    public synchronized <V> void put(K key, V value) {
        put(key, DEFAULT_INDEX, value);

    }

    @Override
    public synchronized <V> V get(K key, Class<V> typeClass) {
        return get(key, DEFAULT_INDEX, -1, typeClass);
    }

    @Override
    public synchronized <V> V get(K key, int ttlMillis, Class<V> typeClass) {
        return get(key, DEFAULT_INDEX, ttlMillis, typeClass);
    }

    @Override
    public synchronized <V> V get(K key, String index, Class<V> typeClass) {
        return get(key, index, -1, typeClass);
    }

    @Override
    public synchronized <V> V get(K key, String index, int ttlMillis, Class<V> typeClass) {
        throwExceptionIfKeyIsInvalid(key);
        zkClientMultiDataUtil.lockForRead(readWriteLock, pathScheme.joinPaths(absoluteBasePath, key.toString(), index),
                this);
        try {
            return zkClientMultiDataUtil.readData(absoluteKeyPath(key), index, ttlMillis, typeClass,
                    readWriteLock == null);
        } finally {
            zkClientMultiDataUtil.unlockForRead(readWriteLock);
        }
    }

    @Override
    public synchronized <V, T extends List<V>> T getAll(K key, Class<V> typeClass) {
        return getAll(key, -1, typeClass);
    }

    @Override
    public synchronized <V, T extends List<V>> T getAll(K key, int ttlMillis, Class<V> typeClass) {
        throwExceptionIfKeyIsInvalid(key);
        zkClientMultiDataUtil.lockForRead(readWriteLock, pathScheme.joinPaths(absoluteBasePath, key.toString()), this);
        try {
            return (T) zkClientMultiDataUtil.readAllData(absoluteKeyPath(key), ttlMillis, typeClass,
                    readWriteLock == null);
        } finally {
            zkClientMultiDataUtil.unlockForRead(readWriteLock);
        }
    }

    @Override
    public synchronized void remove(K key) {
        remove(key, DEFAULT_INDEX, -1);
    }

    @Override
    public synchronized void remove(K key, int ttlMillis) {
        remove(key, DEFAULT_INDEX, ttlMillis);
    }

    @Override
    public synchronized void remove(K key, String index) {
        remove(key, index, -1);
    }

    @Override
    public synchronized void remove(K key, String index, int ttlMillis) {
        throwExceptionIfKeyIsInvalid(key);
        zkClientMultiDataUtil.lockForWrite(readWriteLock);
        try {
            zkClientMultiDataUtil.deleteData(absoluteKeyPath(key), index, ttlMillis, readWriteLock == null);
            deleteKey(key);

        } finally {
            zkClientMultiDataUtil.unlockForWrite(readWriteLock);
        }
    }

    @Override
    public synchronized void removeAll(K key) {
        removeAll(key, -1);
    }

    @Override
    public synchronized void removeAll(K key, int ttlMillis) {
        throwExceptionIfKeyIsInvalid(key);
        zkClientMultiDataUtil.lockForWrite(readWriteLock);
        try {
            zkClientMultiDataUtil.deleteAllData(absoluteKeyPath(key), ttlMillis, readWriteLock == null);
            deleteKey(key);

        } finally {
            zkClientMultiDataUtil.unlockForWrite(readWriteLock);
        }
    }

    @Override
    public synchronized int size() {
        Stat stat = null;
        zkClientMultiDataUtil.lockForRead(readWriteLock, absoluteBasePath, this);
        try {
            if (readWriteLock == null) {
                PathCacheEntry pce = pathCache.get(absoluteBasePath, -1);
                if (pce != null) {
                    stat = pce.getStat();
                }
            }

            if (stat == null) {
                // invalid or non-existent value in cache, so get direct
                stat = new Stat();
                List<String> children = zkClient.getChildren(absoluteBasePath, true, stat);
                pathCache.put(absoluteBasePath, stat, null, children);
            } else {
                logger.trace("Got from cache:  path={}; size={}", absoluteBasePath, stat.getNumChildren());
            }
        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NONODE) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Path does not exist for data update:  " + e + ":  path=" + absoluteBasePath);
                }
            } else {
                logger.error("" + e, e);
            }
        } catch (InterruptedException e) {
            logger.warn("Interrupted:  " + e, e);

        } finally {
            zkClientMultiDataUtil.unlockForRead(readWriteLock);
        }

        return stat != null ? stat.getNumChildren() : -1;
    }

    @Override
    public synchronized List<String> keys() {
        List<String> keys = null;
        zkClientMultiDataUtil.lockForRead(readWriteLock, absoluteBasePath, this);
        try {
            if (readWriteLock == null) {
                keys = zkClientMultiDataUtil.getChildListFromPathCache(absoluteBasePath, -1);
            }

            if (keys == null) {
                // invalid or non-existent value in cache, so get direct
                Stat stat = new Stat();
                keys = zkClient.getChildren(absoluteBasePath, true, stat);
                pathCache.put(absoluteBasePath, stat, null, keys);
            } else {
                logger.trace("Got from cache:  path={}; keys={}", absoluteBasePath, keys);
            }
        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NONODE) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Path does not exist for data update:  " + e + ":  path=" + absoluteBasePath);
                }
            } else {
                logger.error("" + e, e);
            }
        } catch (InterruptedException e) {
            logger.warn("Interrupted:  " + e, e);

        } finally {
            zkClientMultiDataUtil.unlockForRead(readWriteLock);
        }
        return keys;
    }

    /***** private/protected/package *****/

    void deleteKey(K key) {
        try {
            zkClient.delete(absoluteKeyPath(key), -1);
        } catch (Exception e1) {
            logger.error("Trouble deleting key:  key=" + key, e1);
        }
    }

    void throwExceptionIfKeyIsInvalid(K key) {
        if (!pathScheme.isValidToken(key.toString())) {
            throw new IllegalArgumentException("Invalid key:  key='" + key + "'");
        }
    }

    String absoluteKeyPath(K key) {
        return pathScheme.joinPaths(absoluteBasePath, key.toString());
    }

}
