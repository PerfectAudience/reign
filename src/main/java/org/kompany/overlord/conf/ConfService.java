package org.kompany.overlord.conf;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.kompany.overlord.AbstractService;
import org.kompany.overlord.PathType;
import org.kompany.overlord.Sovereign;
import org.kompany.overlord.util.PathCache.PathCacheEntry;
import org.kompany.overlord.util.ZkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfService extends AbstractService implements Watcher {

    private static final Logger logger = LoggerFactory.getLogger(ConfService.class);

    private ZkUtil zkUtil = new ZkUtil();

    private ConcurrentMap<String, ConfObserverWrapper> observerMap = new ConcurrentHashMap<String, ConfObserverWrapper>();

    /**
     * 
     * @param relativePath
     * @param confSerializer
     * @return
     */
    public <T> T getConf(String relativePath, ConfSerializer<T> confSerializer) {
        return getConfAbsolutePath(getPathScheme().getAbsolutePath(PathType.CONF, relativePath), confSerializer, null);

    }

    /**
     * 
     * @param relativePath
     * @param confSerializer
     * @param observer
     * @return
     */
    public <T> T getConf(String relativePath, ConfSerializer<T> confSerializer, ConfObserver<T> observer) {
        return getConfAbsolutePath(getPathScheme().getAbsolutePath(PathType.CONF, relativePath), confSerializer,
                observer);

    }

    /**
     * 
     * @param relativePath
     * @param conf
     * @param confSerializer
     */
    public <T> void putConf(String relativePath, T conf, ConfSerializer<T> confSerializer) {
        putConfAbsolutePath(getPathScheme().getAbsolutePath(PathType.CONF, relativePath), conf, confSerializer,
                Sovereign.DEFAULT_ACL_LIST);
    }

    /**
     * 
     * @param relativePath
     * @param conf
     * @param confSerializer
     * @param aclList
     */
    public <T> void putConf(String relativePath, T conf, ConfSerializer<T> confSerializer, List<ACL> aclList) {
        putConfAbsolutePath(getPathScheme().getAbsolutePath(PathType.CONF, relativePath), conf, confSerializer, aclList);
    }

    /**
     * 
     * @param absolutePath
     * @param confSerializer
     * @return
     */
    <T> T getConfAbsolutePath(String absolutePath, ConfSerializer<T> confSerializer, ConfObserver<T> observer) {
        boolean error = false;
        byte[] bytes = null;
        T result = null;
        try {
            PathCacheEntry pathCacheEntry = getPathCache().get(absolutePath);
            if (pathCacheEntry != null) {
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
            this.observerMap.put(absolutePath, new ConfObserverWrapper<T>(absolutePath, observer, confSerializer,
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
    <T> void putConfAbsolutePath(String absolutePath, T conf, ConfSerializer<T> confSerializer, List<ACL> aclList) {
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
    public void process(WatchedEvent event) {
        // log if DEBUG
        if (logger.isDebugEnabled()) {
            logger.debug("***** Received ZooKeeper Event:  {}",
                    ReflectionToStringBuilder.toString(event, ToStringStyle.DEFAULT_STYLE));

        }

        // check observer map
        String path = event.getPath();
        ConfObserverWrapper observerWrapper = observerMap.get(path);
        if (observerWrapper != null) {
            switch (event.getType()) {
            case NodeChildrenChanged:

                break;
            case NodeCreated:
                observerWrapper.getObserver().handle(
                        getConfAbsolutePath(path, observerWrapper.getConfSerializer(), observerWrapper.getObserver()));
                break;
            case NodeDataChanged:
                Object newValue = getConfAbsolutePath(path, observerWrapper.getConfSerializer(),
                        observerWrapper.getObserver());
                if (newValue != null && !newValue.equals(observerWrapper.getCurrentValue())) {
                    observerWrapper.getObserver().handle(newValue);
                }
                break;
            case NodeDeleted:
                observerWrapper.getObserver().unavailable();
                break;
            case None:
                break;
            default:
                logger.warn("Unhandled event type:  eventType=" + event.getType() + "; eventState=" + event.getState());
            }
        }
    }

    @Override
    public void init() {
        // TODO Auto-generated method stub

    }

    @Override
    public void destroy() {
        // TODO Auto-generated method stub

    }

    private static class ConfObserverWrapper<T> {
        private ConfObserver<T> observer;

        private String path;

        private ConfSerializer<T> confSerializer;

        private T currentValue;

        public ConfObserverWrapper(String path, ConfObserver<T> observer, ConfSerializer<T> confSerializer,
                T currentValue) {
            this.path = path;
            this.observer = observer;
            this.confSerializer = confSerializer;
            this.currentValue = currentValue;
        }

        public ConfObserver<T> getObserver() {
            return observer;
        }

        public String getPath() {
            return path;
        }

        public ConfSerializer<T> getConfSerializer() {
            return confSerializer;
        }

        public T getCurrentValue() {
            return currentValue;
        }

    }

}