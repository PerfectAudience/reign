package io.reign.conf;

import io.reign.AbstractService;
import io.reign.DataSerializer;
import io.reign.JsonDataSerializer;
import io.reign.ObservableService;
import io.reign.PathType;
import io.reign.ServiceObserverManager;
import io.reign.ServiceObserverWrapper;
import io.reign.mesg.RequestMessage;
import io.reign.mesg.ResponseMessage;
import io.reign.util.PathCacheEntry;
import io.reign.util.ZkClientUtil;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Remote/centralized configuration service.
 * 
 * @author ypai
 * 
 */
public class ConfService extends AbstractService implements ObservableService {

    private static final Logger logger = LoggerFactory.getLogger(ConfService.class);

    private final ZkClientUtil zkUtil = new ZkClientUtil();

    private final ServiceObserverManager<ConfObserverWrapper> observerManager = new ServiceObserverManager<ConfObserverWrapper>();

    public <T> void observe(String clusterId, String relativeConfPath, SimpleConfObserver<T> observer) {
        observe(PathType.CONF, clusterId, relativeConfPath, observer);
    }

    <T> void observe(PathType pathType, String clusterId, String relativeConfPath, SimpleConfObserver<T> observer) {

        String absolutePath = getPathScheme().getAbsolutePath(pathType,
                getPathScheme().join(clusterId, relativeConfPath));

        DataSerializer confSerializer = null;
        if (relativeConfPath.endsWith(".properties")) {
            confSerializer = new ConfPropertiesSerializer<ConfProperties>(false);
        } else if (relativeConfPath.endsWith(".json") || relativeConfPath.endsWith(".js")) {
            confSerializer = new JsonDataSerializer();
        } else {
            throw new IllegalArgumentException("Could not derive serializer from given information:  path="
                    + relativeConfPath);
        }

        Object result = getConfValue(absolutePath, confSerializer, false);

        this.observerManager.put(absolutePath, new ConfObserverWrapper(absolutePath, observer, confSerializer, result));
    }

    /**
     * Picks serializer based on path "file extension".
     * 
     * @param <T>
     * @param clusterId
     * @param relativeConfPath
     * @return
     */
    public <T> T getConf(String clusterId, String relativeConfPath) {
        DataSerializer confSerializer = null;
        if (relativeConfPath.endsWith(".properties")) {
            confSerializer = new ConfPropertiesSerializer<ConfProperties>(false);
        } else if (relativeConfPath.endsWith(".json") || relativeConfPath.endsWith(".js")) {
            confSerializer = new JsonDataSerializer<T>();
        } else {
            throw new IllegalArgumentException("Could not derive serializer from given information:  path="
                    + relativeConfPath);
        }
        return (T) getConfAbsolutePath(PathType.CONF, clusterId, relativeConfPath, confSerializer, null, true);

    }

    /**
     * 
     * @param relativePath
     * @param confSerializer
     * @return
     */
    public <T> T getConf(String clusterId, String relativeConfPath, DataSerializer<T> confSerializer) {
        return getConfAbsolutePath(PathType.CONF, clusterId, relativeConfPath, confSerializer, null, true);

    }

    /**
     * 
     * @param relativePath
     * @param confSerializer
     * @param observer
     * @return
     */
    public <T> T getConf(String clusterId, String relativeConfPath, DataSerializer<T> confSerializer,
            SimpleConfObserver<T> observer) {
        return getConfAbsolutePath(PathType.CONF, clusterId, relativeConfPath, confSerializer, observer, true);

    }

    /**
     * Picks serializer based on path "file extension".
     * 
     * @param <T>
     * @param clusterId
     * @param relativeConfPath
     * @param conf
     */
    public <T> void putConf(String clusterId, String relativeConfPath, T conf) {
        DataSerializer confSerializer = null;
        if (relativeConfPath.endsWith(".properties")) {
            confSerializer = new ConfPropertiesSerializer<ConfProperties>(false);
        } else if (relativeConfPath.endsWith(".json") || relativeConfPath.endsWith(".js")) {
            confSerializer = new JsonDataSerializer<T>();
        } else {
            throw new IllegalArgumentException("Could not derive serializer from given information:  path="
                    + relativeConfPath);
        }
        putConfAbsolutePath(PathType.CONF, clusterId, relativeConfPath, conf, confSerializer, getDefaultAclList());
    }

    /**
     * 
     * @param relativePath
     * @param conf
     * @param confSerializer
     */
    public <T> void putConf(String clusterId, String relativeConfPath, T conf, DataSerializer<T> confSerializer) {
        putConfAbsolutePath(PathType.CONF, clusterId, relativeConfPath, conf, confSerializer, getDefaultAclList());
    }

    /**
     * 
     * @param relativePath
     * @param conf
     * @param confSerializer
     * @param aclList
     */
    public <T> void putConf(String clusterId, String relativeConfPath, T conf, DataSerializer<T> confSerializer,
            List<ACL> aclList) {
        putConfAbsolutePath(PathType.CONF, clusterId, relativeConfPath, conf, confSerializer, aclList);
    }

    /**
     * 
     * @param relativePath
     */
    public void removeConf(String clusterId, String relativeConfPath) {
        String path = getPathScheme().getAbsolutePath(PathType.CONF, getPathScheme().join(clusterId, relativeConfPath));
        try {
            getZkClient().delete(path, -1);

            // set up watch for when path comes back if there are observers
            if (this.observerManager.isBeingObserved(path)) {
                getZkClient().exists(path, true);
            }

        } catch (KeeperException e) {
            if (e.code() != Code.NONODE) {
                logger.error("removeConf():  error trying to delete node:  " + e + ":  path=" + path, e);
            }
        } catch (Exception e) {
            logger.error("removeConf():  error trying to delete node:  " + e + ":  path=" + path, e);
        }
    }

    /**
     * 
     * @param <T>
     * @param pathContext
     * @param pathType
     * @param clusterId
     * @param relativeConfPath
     * @param confSerializer
     * @param observer
     * @param useCache
     * @return
     */
    <T> T getConfAbsolutePath(PathType pathType, String clusterId, String relativeConfPath,
            DataSerializer<T> confSerializer, SimpleConfObserver<T> observer, boolean useCache) {
        String absolutePath = getPathScheme().getAbsolutePath(pathType,
                getPathScheme().join(clusterId, relativeConfPath));
        return getConfAbsolutePath(absolutePath, confSerializer, observer, useCache);

    }

    /**
     * 
     * @param <T>
     * @param absolutePath
     * @param confSerializer
     * @param observer
     * @param useCache
     * @return
     */
    <T> T getConfAbsolutePath(String absolutePath, DataSerializer<T> confSerializer, SimpleConfObserver<T> observer,
            boolean useCache) {

        T result = getConfValue(absolutePath, confSerializer, useCache && observer == null);

        /** add observer if given **/
        if (observer != null) {
            this.observerManager.put(absolutePath, new ConfObserverWrapper<T>(absolutePath, observer, confSerializer,
                    result));

        }

        return result;
    }

    <T> T getConfValue(String absolutePath, DataSerializer<T> confSerializer, boolean useCache) {
        byte[] bytes = null;
        T result = null;

        try {
            PathCacheEntry pathCacheEntry = getPathCache().get(absolutePath);
            if (useCache && pathCacheEntry != null) {
                // found in cache
                bytes = pathCacheEntry.getBytes();
            } else {
                // not in cache, so load from ZK
                Stat stat = new Stat();
                bytes = getZkClient().getData(absolutePath, true, stat);

                // put in cache
                getPathCache().put(absolutePath, stat, bytes, null);
            }

            result = bytes != null ? confSerializer.deserialize(bytes) : null;

        } catch (KeeperException e) {
            if (e.code() == Code.NONODE) {
                // set up watch on that node
                try {
                    getZkClient().exists(absolutePath, true);
                } catch (Exception e1) {
                    logger.error("getConfValue():  error trying to watch node:  " + e1 + ":  path=" + absolutePath, e1);
                }

                logger.debug("getConfValue():  error trying to fetch node info:  {}:  node does not exist:  path={}",
                        e.getMessage(), absolutePath);
            } else {
                logger.error("getConfValue():  error trying to fetch node info:  " + e, e);
            }
        } catch (Exception e) {
            logger.error("getConfValue():  error trying to fetch node info:  " + e, e);
        }

        return result;
    }

    /**
     * 
     * @param <T>
     * @param pathContext
     * @param pathType
     * @param clusterId
     * @param relativeConfPath
     * @param conf
     * @param confSerializer
     * @param aclList
     */
    <T> void putConfAbsolutePath(PathType pathType, String clusterId, String relativeConfPath, T conf,
            DataSerializer<T> confSerializer, List<ACL> aclList) {
        String absolutePath = getPathScheme().getAbsolutePath(pathType,
                getPathScheme().join(clusterId, relativeConfPath));
        try {
            // write to ZK
            byte[] leafData = confSerializer.serialize(conf);
            String pathUpdated = zkUtil.updatePath(getZkClient(), getPathScheme(), absolutePath, leafData, aclList,
                    CreateMode.PERSISTENT, -1);

            logger.debug("putConfAbsolutePath():  saved configuration:  path={}", pathUpdated);
        } catch (Exception e) {
            logger.error("putConfAbsolutePath():  error while saving configuration:  " + e + ":  path=" + absolutePath,
                    e);
        }
    }

    @Override
    public void signalStateReset(Object o) {
        this.observerManager.signalStateReset(o);
    }

    @Override
    public void signalStateUnknown(Object o) {
        this.observerManager.signalStateUnknown(o);
    }

    @Override
    public boolean filterWatchedEvent(WatchedEvent event) {
        return !observerManager.isBeingObserved(event.getPath());

    }

    @Override
    public void nodeCreated(WatchedEvent event) {
        nodeDataChanged(event);
    }

    @Override
    public void nodeDataChanged(WatchedEvent event) {
        String path = event.getPath();
        ConfObserverWrapper<SimpleConfObserver> observerWrapper = observerManager.getObserverWrapperSet(path)
                .iterator().next();
        Object newValue = getConfAbsolutePath(path, observerWrapper.getDataSerializer(), null, true);
        if (newValue != null && !newValue.equals(observerWrapper.getCurrentValue())) {
            observerManager.signal(path, newValue);
        }
    }

    @Override
    public void nodeDeleted(WatchedEvent event) {
        observerManager.signal(event.getPath(), null);
    }

    @Override
    public void connected(WatchedEvent event) {
        this.signalStateReset(null);
    }

    @Override
    public void sessionExpired(WatchedEvent event) {
        this.signalStateUnknown(null);
    }

    @Override
    public void disconnected(WatchedEvent event) {
        this.signalStateUnknown(null);
    }

    @Override
    public void init() {
    }

    @Override
    public void destroy() {
    }

    /**
     * TODO: implement!
     */
    @Override
    public ResponseMessage handleMessage(RequestMessage requestMessage) {
        return null;
    }

    private static class ConfObserverWrapper<T> extends ServiceObserverWrapper<SimpleConfObserver<T>> {

        // private String path;

        private final DataSerializer<T> dataSerializer;

        private volatile T currentValue;

        public ConfObserverWrapper(String path, SimpleConfObserver<T> observer, DataSerializer<T> dataSerializer,
                T currentValue) {
            // from super class
            this.observer = observer;

            // this.path = path;
            this.dataSerializer = dataSerializer;
            this.currentValue = currentValue;
        }

        @Override
        public void signalObserver(Object o) {
            this.observer.updated((T) o);

            // update current value for comparison against any future events
            // (sometimes we get a ZK event even if relevant value has not
            // changed: for example, when updating node data with the exact same
            // value)
            this.currentValue = (T) o;
        }

        // public String getPath() {
        // return path;
        // }

        public DataSerializer<T> getDataSerializer() {
            return dataSerializer;
        }

        public T getCurrentValue() {
            return currentValue;
        }

    }// private

}