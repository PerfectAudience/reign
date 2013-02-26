package org.kompany.overlord.conf;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.kompany.overlord.AbstractService;
import org.kompany.overlord.DataSerializer;
import org.kompany.overlord.ObservableService;
import org.kompany.overlord.PathContext;
import org.kompany.overlord.PathType;
import org.kompany.overlord.ServiceObserverManager;
import org.kompany.overlord.ServiceObserverWrapper;
import org.kompany.overlord.Sovereign;
import org.kompany.overlord.util.PathCache.PathCacheEntry;
import org.kompany.overlord.util.ZkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfService extends AbstractService implements ObservableService, Watcher {

    private static final Logger logger = LoggerFactory.getLogger(ConfService.class);

    private final ZkUtil zkUtil = new ZkUtil();

    private final ServiceObserverManager<ConfObserverWrapper> observerManager = new ServiceObserverManager<ConfObserverWrapper>();

    /**
     * 
     * @param relativePath
     * @param confSerializer
     * @return
     */
    public <T> T getConf(String relativePath, DataSerializer<T> confSerializer) {
        return getConfAbsolutePath(getPathScheme().getAbsolutePath(PathContext.USER, PathType.CONF, relativePath),
                confSerializer, null, true);

    }

    /**
     * 
     * @param relativePath
     * @param confSerializer
     * @param observer
     * @return
     */
    public <T> T getConf(String relativePath, DataSerializer<T> confSerializer, ConfObserver<T> observer) {
        return getConfAbsolutePath(getPathScheme().getAbsolutePath(PathContext.USER, PathType.CONF, relativePath),
                confSerializer, observer, true);

    }

    /**
     * 
     * @param relativePath
     * @param conf
     * @param confSerializer
     */
    public <T> void putConf(String relativePath, T conf, DataSerializer<T> confSerializer) {
        putConfAbsolutePath(getPathScheme().getAbsolutePath(PathContext.USER, PathType.CONF, relativePath), conf,
                confSerializer, Sovereign.DEFAULT_ACL_LIST);
    }

    /**
     * 
     * @param relativePath
     * @param conf
     * @param confSerializer
     * @param aclList
     */
    public <T> void putConf(String relativePath, T conf, DataSerializer<T> confSerializer, List<ACL> aclList) {
        putConfAbsolutePath(getPathScheme().getAbsolutePath(PathContext.USER, PathType.CONF, relativePath), conf,
                confSerializer, aclList);
    }

    /**
     * 
     * @param relativePath
     */
    public void removeConf(String relativePath) {
        String path = getPathScheme().getAbsolutePath(PathContext.USER, PathType.CONF, relativePath);
        try {
            getZkClient().delete(path, -1);

            // set up watch for when path comes back if there are
            // observers
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
     * @param absolutePath
     * @param confSerializer
     * @return
     */
    public <T> T getConfAbsolutePath(String absolutePath, DataSerializer<T> confSerializer, ConfObserver<T> observer,
            boolean useCache) {
        boolean error = false;
        byte[] bytes = null;
        T result = null;
        try {
            PathCacheEntry pathCacheEntry = getPathCache().get(absolutePath);
            if (useCache && observer == null && pathCacheEntry != null) {
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
                    logger.error("getConfAbsolutePath():  error trying to watch node:  " + e1 + ":  path="
                            + absolutePath, e1);
                }

                logger.debug(
                        "getConfAbsolutePath():  error trying to fetch node info:  {}:  node does not exist:  path={}",
                        e.getMessage(), absolutePath);
            } else {
                logger.error("getConfAbsolutePath():  error trying to fetch node info:  " + e, e);
            }
            error = true;
        } catch (Exception e) {
            logger.error("getConfAbsolutePath():  error trying to fetch node info:  " + e, e);
            error = true;
        }

        /** add observer if given **/
        if (observer != null) {
            this.observerManager.put(absolutePath, new ConfObserverWrapper<T>(absolutePath, observer, confSerializer,
                    result));

        }

        return error ? null : result;

    }

    /**
     * 
     * @param absolutePath
     * @param conf
     * @param confSerializer
     * @param aclList
     */
    public <T> void putConfAbsolutePath(String absolutePath, T conf, DataSerializer<T> confSerializer, List<ACL> aclList) {
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
        ConfObserverWrapper<ConfObserver> observerWrapper = observerManager.getObserverWrapperSet(path).iterator()
                .next();
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

    private static class ConfObserverWrapper<T> extends ServiceObserverWrapper<ConfObserver<T>> {

        // private String path;

        private final DataSerializer<T> dataSerializer;

        private volatile T currentValue;

        public ConfObserverWrapper(String path, ConfObserver<T> observer, DataSerializer<T> dataSerializer,
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