package io.reign.data;

import io.reign.DataSerializer;
import io.reign.PathScheme;
import io.reign.ZkClient;
import io.reign.coord.DistributedReadWriteLock;
import io.reign.util.PathCache;
import io.reign.util.PathCacheEntry;
import io.reign.util.ZkClientUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 * @param <V>
 */
public class ZkMultiData<V> implements MultiData<V> {

    private static final Logger logger = LoggerFactory.getLogger(ZkMultiData.class);

    private final ZkClientUtil zkClientUtil;

    private final Map<String, DataSerializer> dataSerializerMap;

    private final DistributedReadWriteLock readWriteLock;

    private final String absoluteBasePath;

    private final PathScheme pathScheme;

    private final ZkClient zkClient;

    private final PathCache pathCache;

    private final List<ACL> aclList;

    private final Class<V> typeClass;

    /**
     * 
     * @param relativeBasePath
     * @param readWriteLock
     *            for inter-process safety; can be null
     */
    public ZkMultiData(Class<V> typeClass, String absoluteBasePath, PathScheme pathScheme,
            DistributedReadWriteLock readWriteLock, ZkClient zkClient, PathCache pathCache, ZkClientUtil zkClientUtil,
            Map<String, DataSerializer> dataSerializerMap, List<ACL> aclList) {
        this.typeClass = typeClass;
        this.absoluteBasePath = absoluteBasePath;
        this.pathScheme = pathScheme;
        this.readWriteLock = readWriteLock;
        this.zkClient = zkClient;
        this.pathCache = pathCache;
        this.zkClientUtil = zkClientUtil;
        this.dataSerializerMap = dataSerializerMap;
        this.aclList = aclList;

    }

    @Override
    public synchronized void destroy() {
        if (readWriteLock != null) {
            readWriteLock.destroy();
        }
    }

    @Override
    public synchronized void set(V value) {
        set(DataValue.DEFAULT_INDEX, value);
    }

    @Override
    public synchronized void set(String index, V value) {
        lockForWrite();
        try {
            writeData(absoluteBasePath, index, value);
        } finally {
            unlockForWrite();
        }

    }

    @Override
    public synchronized V get() {
        return get(DataValue.DEFAULT_INDEX);

    }

    @Override
    public synchronized V get(String index) {
        lockForRead(pathScheme.joinPaths(absoluteBasePath, index));
        try {
            return readData(absoluteBasePath, index, -1);
        } finally {
            unlockForRead();
        }

    }

    @Override
    public synchronized List<V> getAll() {
        lockForRead(absoluteBasePath);
        try {
            return readAllData(absoluteBasePath, -1);
        } finally {
            unlockForRead();
        }

    }

    @Override
    public synchronized String remove() {
        return remove(DataValue.DEFAULT_INDEX);
    }

    @Override
    public synchronized String remove(String index) {
        lockForWrite();
        try {
            return remove(absoluteBasePath, index, -1);
        } finally {
            unlockForWrite();
        }

    }

    @Override
    public synchronized List<String> removeAll() {
        lockForWrite();
        try {
            return removeAll(absoluteBasePath, -1);
        } finally {
            unlockForWrite();
        }

    }

    void lockForWrite() {
        if (readWriteLock != null) {
            readWriteLock.writeLock().lock();
        }// if
    }

    void unlockForWrite() {
        if (readWriteLock != null) {
            readWriteLock.writeLock().unlock();
        }
    }

    void lockForRead(String dataPath) {
        if (readWriteLock != null) {
            readWriteLock.readLock().lock();
            syncZkClient(dataPath);
        }
    }

    void unlockForRead() {
        if (readWriteLock != null) {
            readWriteLock.readLock().unlock();
        }
    }

    void syncZkClient(String dataPath) {
        logger.trace("Syncing ZK client:  dataPath={}", dataPath);
        zkClient.sync(dataPath, new VoidCallback() {
            @Override
            public void processResult(int arg0, String arg1, Object arg2) {
                synchronized (ZkMultiData.this) {
                    ZkMultiData.this.notifyAll();
                }
            }
        }, null);

        // wait for sync to complete
        synchronized (this) {
            try {
                logger.trace("Waiting for ZK client sync complete:  dataPath={}", dataPath);
                this.wait();
                logger.trace("ZK client sync completed:  dataPath={}", dataPath);
            } catch (InterruptedException e) {
                logger.warn("Interrupted while waiting for ZK sync():  " + e, e);
            }
        }
    }

    String writeData(String absoluteBasePath, String index, V value) {
        try {

            byte[] bytes = null;
            if (value != null) {
                DataSerializer<V> dataSerializer = dataSerializerMap.get(typeClass.getName());
                if (dataSerializer == null) {
                    throw new IllegalStateException("No data serializer/deserializer found for " + typeClass.getName());
                }
                bytes = dataSerializer.serialize(value);
            }

            // write data to ZK
            AtomicReference<Stat> statRef = new AtomicReference<Stat>();
            String absoluteDataValuePath = zkClientUtil.updatePath(zkClient, pathScheme, pathScheme.joinPaths(
                    absoluteBasePath, index), bytes, aclList, CreateMode.PERSISTENT, -1, statRef);

            // logger.debug("writeData():  absoluteBasePath={}; index={}; absoluteDataValuePath={}; value={}",
            // new Object[] { absoluteBasePath, index, absoluteDataValuePath, value });

            // get stat if it was not returned from updatePath
            Stat stat = statRef.get();
            if (stat == null) {
                stat = zkClient.exists(absoluteDataValuePath, false);
            }

            // update stat with recent update time in case we have a stale read
            stat.setMtime(System.currentTimeMillis());

            // update path cache after successful write
            pathCache.put(absoluteDataValuePath, stat, bytes, null);

            PathCacheEntry pce = pathCache.get(absoluteDataValuePath);
            logger.debug("writeData():  absoluteDataValuePath={}; pathCacheEntry={}", absoluteDataValuePath,
                    dataSerializerMap.get(typeClass.getName()).deserialize(pce.getBytes()));

            return absoluteDataValuePath;

        } catch (KeeperException e) {
            logger.error("" + e, e);
            return null;
        } catch (Exception e) {
            logger.error("" + e, e);
            return null;
        }
    }

    /**
     * 
     * @param absoluteBasePath
     * @param index
     * @param thresholdMillis
     *            remove data older than given threshold
     * @return
     */
    String remove(String absoluteBasePath, String index, int thresholdMillis) {
        try {
            String absoluteDataPath = pathScheme.joinPaths(absoluteBasePath, index);

            // try to get from path cache, use stat modified timestamp instead of cache entry modified timestamp because
            // we are more interested in when the data last changed
            byte[] bytes = null;
            if (thresholdMillis > 0) {
                bytes = getDataFromPathCache(absoluteDataPath, thresholdMillis);
                if (bytes == null) {
                    // read data from ZK
                    Stat stat = new Stat();
                    bytes = zkClient.getData(absoluteDataPath, true, stat);

                    // see if item is expired
                    if (isExpired(stat.getMtime(), thresholdMillis)) {
                        bytes = null;
                    }
                }
            }

            // if bytes == null, meaning that data for this index is expired or that we are removing regardless of data
            // age, delete data node
            String deletedPath = null;
            if (bytes == null) {
                zkClient.delete(absoluteDataPath, -1);
                deletedPath = absoluteDataPath;

                // remove node entry in path cache
                pathCache.remove(absoluteDataPath);

                // update parent children in path cache if parent node exists in cache
                String absoluteParentPath = pathScheme.getParentPath(absoluteDataPath);
                PathCacheEntry pathCacheEntry = pathCache.get(absoluteParentPath);
                if (pathCacheEntry != null) {
                    List<String> currentChildList = pathCacheEntry.getChildren();
                    List<String> newChildList = new ArrayList<String>(currentChildList.size());
                    for (String child : currentChildList) {
                        if (!child.equals(index)) {
                            newChildList.add(child);
                        }
                    }
                    pathCache
                            .put(absoluteParentPath, pathCacheEntry.getStat(), pathCacheEntry.getBytes(), newChildList);
                }
            }

            return deletedPath;

        } catch (KeeperException e) {
            logger.error("" + e, e);
            return null;
        } catch (Exception e) {
            logger.error("" + e, e);
            return null;
        }
    }

    List<String> removeAll(String absoluteBasePath, int thresholdMillis) {
        try {
            // get children
            List<String> childList = getChildrenFromPathCache(absoluteBasePath, thresholdMillis);
            if (childList == null) {
                Stat stat = new Stat();
                childList = zkClient.getChildren(absoluteBasePath, true, stat);

                // update in path cache
                pathCache.put(absoluteBasePath, stat, null, childList);
            }

            // iterate through children and build up list
            if (childList.size() > 0) {
                List<String> resultList = new ArrayList<String>(childList.size());
                for (String child : childList) {
                    String deletedPath = remove(absoluteBasePath, child, thresholdMillis);

                    // see if we deleted
                    if (deletedPath != null) {
                        resultList.add(deletedPath);
                    }
                }// for

                return resultList;
            } // if

            // return list
            return Collections.EMPTY_LIST;

        } catch (KeeperException e) {
            logger.error("" + e, e);
            return Collections.EMPTY_LIST;
        } catch (Exception e) {
            logger.error("" + e, e);
            return Collections.EMPTY_LIST;
        }
    }

    V readData(String absoluteBasePath, String index, int ttlMillis) {
        try {
            String absoluteDataPath = pathScheme.joinPaths(absoluteBasePath, index);

            // try to get from path cache, use stat modified timestamp instead of cache entry modified timestamp because
            // we are more interested in when the data last changed
            byte[] bytes = getDataFromPathCache(absoluteDataPath, ttlMillis);
            if (bytes == null) {
                // read data from ZK
                Stat stat = new Stat();
                bytes = zkClient.getData(absoluteDataPath, true, stat);

                // see if item is expired
                if (isExpired(stat.getMtime(), ttlMillis)) {
                    return null;
                }

                // update in path cache
                pathCache.put(absoluteDataPath, stat, bytes, Collections.EMPTY_LIST);
            }

            // deserialize
            V data = null;
            if (bytes != null && bytes != EMPTY_BYTE_ARRAY) {
                DataSerializer<V> dataSerializer = dataSerializerMap.get(typeClass.getName());
                if (dataSerializer == null) {
                    throw new IllegalStateException("No data serializer/deserializer found for " + typeClass.getName());
                }
                data = dataSerializer.deserialize(bytes);
            }

            // logger.debug("readData():  absoluteBasePath={}; index={}; value={}", new Object[] { absoluteBasePath,
            // index, data });

            return data;

        } catch (KeeperException e) {
            logger.error("" + e, e);
            return null;
        } catch (Exception e) {
            logger.error("" + e, e);
            return null;
        }
    }

    /**
     * 
     * @param absoluteBasePath
     * @param ttlMillis
     * @return List of children; or null if data in cache is expired or missing
     */
    List<String> getChildrenFromPathCache(String absoluteBasePath, int ttlMillis) {
        // if process safe, we never fetch from cache
        if (readWriteLock != null) {
            return null;
        }

        List<String> childList = null;
        PathCacheEntry pathCacheEntry = pathCache.get(absoluteBasePath);
        if (pathCacheEntry != null && !isExpired(pathCacheEntry.getLastUpdatedTimestampMillis(), ttlMillis)) {
            childList = pathCacheEntry.getChildren();
        }

        return childList;
    }

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    /**
     * 
     * @param absoluteDataPath
     * @param ttlMillis
     * @return byte[] or null if data in cache is expired or missing
     */
    byte[] getDataFromPathCache(String absoluteDataPath, int ttlMillis) {
        // if process safe, we never fetch from cache
        if (readWriteLock != null) {
            return null;
        }

        byte[] bytes = null;
        PathCacheEntry pathCacheEntry = pathCache.get(absoluteDataPath);
        if (pathCacheEntry != null && !isExpired(pathCacheEntry.getStat().getMtime(), ttlMillis)) {
            bytes = pathCacheEntry.getBytes();

            // valid value, but we need a way in this use case to distinguish btw. expired/missing value in pathCache
            // (return null) and
            // valid value in pathCache but empty
            if (bytes == null) {
                bytes = EMPTY_BYTE_ARRAY;
            }
        }
        return bytes;
    }

    List<V> readAllData(String absoluteBasePath, int ttlMillis) {

        try {
            // get children
            List<String> childList = getChildrenFromPathCache(absoluteBasePath, ttlMillis);
            if (childList == null) {
                Stat stat = new Stat();
                childList = zkClient.getChildren(absoluteBasePath, true, stat);

                // update in path cache
                pathCache.put(absoluteBasePath, stat, null, childList);
            }

            // iterate through children and build up list
            if (childList.size() > 0) {
                List<V> resultList = new ArrayList<V>(childList.size());
                for (String child : childList) {
                    V value = readData(absoluteBasePath, child, ttlMillis);

                    // logger.debug("readAllData():  absoluteBasePath={}; index={}; value={}", new Object[] {
                    // absoluteBasePath, child, value });

                    if (value != null) {
                        resultList.add(value);
                    }
                }// for

                return resultList;
            } // if

            // return list
            return Collections.EMPTY_LIST;

        } catch (KeeperException e) {
            logger.error("" + e, e);
            return Collections.EMPTY_LIST;
        } catch (Exception e) {
            logger.error("" + e, e);
            return Collections.EMPTY_LIST;
        }
    }

    boolean isExpired(long lastModifiedMillis, int ttlMillis) {
        return ttlMillis > 0 && lastModifiedMillis + ttlMillis < System.currentTimeMillis();
    }
}
