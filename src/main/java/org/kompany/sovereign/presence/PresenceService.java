package org.kompany.sovereign.presence;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.kompany.sovereign.AbstractActiveService;
import org.kompany.sovereign.ObservableService;
import org.kompany.sovereign.PathContext;
import org.kompany.sovereign.PathType;
import org.kompany.sovereign.ServiceObserverManager;
import org.kompany.sovereign.ServiceObserverWrapper;
import org.kompany.sovereign.coord.CoordinationService;
import org.kompany.sovereign.coord.DistributedLock;
import org.kompany.sovereign.util.PathCacheEntry;
import org.kompany.sovereign.util.ZkClientUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service discovery service.
 * 
 * @author ypai
 * 
 */
public class PresenceService extends AbstractActiveService implements ObservableService {

    private static final Logger logger = LoggerFactory.getLogger(PresenceService.class);

    public static final int DEFAULT_ZOMBIE_CHECK_INTERVAL_MILLIS = 300000;

    public static final int DEFAULT_HEARTBEAT_INTERVAL_MILLIS = 30000;

    public static final int DEFAULT_EXECUTION_INTERVAL_MILLIS = 2000;

    private int pathCacheSize = 250;

    private int concurrencyLevel = 8;

    private int heartbeatIntervalMillis = DEFAULT_HEARTBEAT_INTERVAL_MILLIS;

    private int zombieCheckIntervalMillis = DEFAULT_ZOMBIE_CHECK_INTERVAL_MILLIS;

    private NodeAttributeSerializer nodeAttributeSerializer = new JsonNodeAttributeSerializer();

    private final ConcurrentMap<String, Announcement> announcementMap = new ConcurrentHashMap<String, Announcement>(8,
            0.9f, 2);

    private final ConcurrentMap<String, PresenceObserver> notifyObserverMap = new ConcurrentHashMap<String, PresenceObserver>(
            8, 0.9f, 2);

    private final ServiceObserverManager<PresenceObserverWrapper> observerManager = new ServiceObserverManager<PresenceObserverWrapper>();

    private final ZkClientUtil zkUtil = new ZkClientUtil();

    private volatile long lastZombieCheckTimestamp = System.currentTimeMillis();

    public List<String> getAvailableServices(String clusterId) {
        /** get node data from zk **/
        String clusterPath = getPathScheme().buildRelativePath(clusterId);
        String path = getPathScheme().getAbsolutePath(PathContext.USER, PathType.PRESENCE, clusterPath);
        List<String> children = null;
        try {
            Stat stat = new Stat();
            children = getZkClient().getChildren(path, true, stat);

            getPathCache().put(path, stat, null, children);

        } catch (KeeperException e) {
            if (e.code() == Code.NONODE) {
                logger.warn("getAvailableServices():  " + e + ":  node does not exist:  path={}", path);
            } else {
                logger.warn("getAvailableServices():  " + e, e);
            }
            return null;
        } catch (InterruptedException e) {
            logger.warn("getAvailableServices():  " + e, e);
            return null;
        }

        return children;
    }

    public void observe(String clusterId, String serviceId, PresenceObserver<ServiceInfo> observer) {
        ServiceInfo result = lookup(clusterId, serviceId, observer, nodeAttributeSerializer, true);
        String servicePath = getPathScheme().buildRelativePath(clusterId, serviceId);
        String path = getPathScheme().getAbsolutePath(PathContext.USER, PathType.PRESENCE, servicePath);
        this.observerManager.put(path, new PresenceObserverWrapper<ServiceInfo>(clusterId, serviceId, null, observer,
                nodeAttributeSerializer, result));
    }

    public void observe(String clusterId, String serviceId, String nodeId, PresenceObserver<NodeInfo> observer) {
        NodeInfo result = lookup(clusterId, serviceId, nodeId, observer, nodeAttributeSerializer, true);
        String nodePath = getPathScheme().buildRelativePath(clusterId, serviceId, nodeId);
        String path = getPathScheme().getAbsolutePath(PathContext.USER, PathType.PRESENCE, nodePath);
        this.observerManager.put(path, new PresenceObserverWrapper<NodeInfo>(clusterId, serviceId, nodeId, observer,
                nodeAttributeSerializer, result));
    }

    public ServiceInfo waitUntilAvailable(String clusterId, String serviceId, long timeoutMillis) {
        return waitUntilAvailable(clusterId, serviceId, null, null, true, timeoutMillis);
    }

    public ServiceInfo waitUntilAvailable(String clusterId, String serviceId, PresenceObserver<ServiceInfo> observer,
            long timeoutMillis) {
        return waitUntilAvailable(clusterId, serviceId, observer, null, true, timeoutMillis);
    }

    /**
     * 
     * @param clusterId
     * @param serviceId
     * @param observer
     * @param nodeAttributeSerializer
     * @param useCache
     * @param timeoutMillis
     *            set to <0 to wait indefinitely
     * @return
     */
    public ServiceInfo waitUntilAvailable(String clusterId, String serviceId, PresenceObserver<ServiceInfo> observer,
            NodeAttributeSerializer nodeAttributeSerializer, boolean useCache, long timeoutMillis) {
        ServiceInfo result = lookup(clusterId, serviceId, observer, nodeAttributeSerializer, useCache);
        if (result == null) {
            String servicePath = getPathScheme().buildRelativePath(clusterId, serviceId);
            String path = getPathScheme().getAbsolutePath(PathContext.USER, PathType.PRESENCE, servicePath);
            PresenceObserver<ServiceInfo> notifyObserver = getNotifyObserver(path);
            this.observerManager.put(path, new PresenceObserverWrapper<ServiceInfo>(clusterId, serviceId, null,
                    notifyObserver, nodeAttributeSerializer, result));
            synchronized (notifyObserver) {
                long waitStartTimestamp = System.currentTimeMillis();
                while (result == null
                        && (System.currentTimeMillis() - waitStartTimestamp < timeoutMillis || timeoutMillis < 0)) {
                    try {
                        if (timeoutMillis < 0) {
                            notifyObserver.wait();
                        } else {
                            notifyObserver.wait(timeoutMillis);
                        }
                    } catch (InterruptedException e) {
                        logger.warn("Interrupted while waiting for ServiceInfo:  " + e, e);
                    }
                    result = lookup(clusterId, serviceId, observer, nodeAttributeSerializer, useCache);
                }
            }

        }
        return result;
    }

    <T> PresenceObserver<T> getNotifyObserver(final String path) {
        // create announcement if necessary
        PresenceObserver<T> observer = notifyObserverMap.get(path);
        if (observer == null) {
            PresenceObserver<T> newObserver = new PresenceObserver<T>() {
                @Override
                public void updated(T info) {
                    logger.debug("Notifying all waiters [{}]:  path={}", this.hashCode(), path);
                    synchronized (this) {
                        this.notifyAll();
                    }
                }
            };

            observer = notifyObserverMap.putIfAbsent(path, newObserver);
            if (observer == null) {
                observer = newObserver;
            }
        }
        return observer;
    }

    public ServiceInfo lookup(String clusterId, String serviceId) {
        return lookup(clusterId, serviceId, null, nodeAttributeSerializer, true);
    }

    public ServiceInfo lookup(String clusterId, String serviceId, PresenceObserver<ServiceInfo> observer) {
        return lookup(clusterId, serviceId, observer, nodeAttributeSerializer, true);
    }

    public ServiceInfo lookup(String clusterId, String serviceId, PresenceObserver<ServiceInfo> observer,
            NodeAttributeSerializer nodeAttributeSerializer, boolean useCache) {
        /** get node data from zk **/
        String servicePath = getPathScheme().buildRelativePath(clusterId, serviceId);
        String path = getPathScheme().getAbsolutePath(PathContext.USER, PathType.PRESENCE, servicePath);

        boolean error = false;

        List<String> children = null;
        try {
            PathCacheEntry pathCacheEntry = getPathCache().get(path);
            if (useCache && observer == null && pathCacheEntry != null) {
                // found in cache
                children = pathCacheEntry.getChildren();
            } else {
                // not in cache, so populate from ZK
                Stat stat = new Stat();
                children = getZkClient().getChildren(path, true, stat);

                // update cache
                getPathCache().put(path, stat, null, children);
            }

        } catch (KeeperException e) {
            if (e.code() == Code.NONODE) {
                // set up watch on that node
                try {
                    getZkClient().exists(path, true);
                } catch (Exception e1) {
                    logger.error("lookup():  error trying to watch node:  " + e1 + ":  path=" + path, e1);
                }

                logger.debug("lookup():  error trying to fetch service info:  {}:  node does not exist:  path={}",
                        e.getMessage(), path);
            } else {
                logger.error("lookup():  error trying to fetch service info:  " + e, e);
            }
            error = true;
        } catch (Exception e) {
            logger.warn("lookup():  error trying to fetch service info:  " + e, e);
            error = true;
        }

        /** build service info **/
        ServiceInfo result = null;
        if (!error) {
            result = new ServiceInfo(clusterId, serviceId, children);
        }

        /** add observer if given **/
        if (observer != null) {
            this.observerManager.put(path, new PresenceObserverWrapper<ServiceInfo>(clusterId, serviceId, null,
                    observer, nodeAttributeSerializer, result));
        }

        return result;

    }

    public NodeInfo waitUntilAvailable(String clusterId, String serviceId, String nodeId, long timeoutMillis) {
        return waitUntilAvailable(clusterId, serviceId, nodeId, null, nodeAttributeSerializer, true, timeoutMillis);
    }

    public NodeInfo waitUntilAvailable(String clusterId, String serviceId, String nodeId,
            PresenceObserver<NodeInfo> observer, long timeoutMillis) {
        return waitUntilAvailable(clusterId, serviceId, nodeId, observer, nodeAttributeSerializer, true, timeoutMillis);
    }

    /**
     * 
     * @param clusterId
     * @param serviceId
     * @param nodeId
     * @param observer
     * @param nodeAttributeSerializer
     * @param useCache
     * @param timeoutMillis
     *            set to <0 to wait indefinitely
     * @return
     */
    public NodeInfo waitUntilAvailable(String clusterId, String serviceId, String nodeId,
            PresenceObserver<NodeInfo> observer, NodeAttributeSerializer nodeAttributeSerializer, boolean useCache,
            long timeoutMillis) {
        NodeInfo result = lookup(clusterId, serviceId, nodeId, observer, nodeAttributeSerializer, useCache);
        if (result == null) {
            String nodePath = getPathScheme().buildRelativePath(clusterId, serviceId, nodeId);
            String path = getPathScheme().getAbsolutePath(PathContext.USER, PathType.PRESENCE, nodePath);
            PresenceObserver<NodeInfo> notifyObserver = getNotifyObserver(path);
            this.observerManager.put(path, new PresenceObserverWrapper<NodeInfo>(clusterId, serviceId, nodeId,
                    notifyObserver, nodeAttributeSerializer, result));
            synchronized (notifyObserver) {
                long waitStartTimestamp = System.currentTimeMillis();
                while (result == null
                        && (System.currentTimeMillis() - waitStartTimestamp < timeoutMillis || timeoutMillis < 0)) {
                    try {
                        if (timeoutMillis < 0) {
                            notifyObserver.wait();
                        } else {
                            notifyObserver.wait(timeoutMillis);
                        }
                    } catch (InterruptedException e) {
                        logger.warn("Interrupted while waiting for NodeInfo:  " + e, e);
                    }
                    result = lookup(clusterId, serviceId, nodeId, observer, nodeAttributeSerializer, useCache);
                }
            }

        }

        return result;
    }

    public NodeInfo lookup(String clusterId, String serviceId, String nodeId) {
        return lookup(clusterId, serviceId, nodeId, null, nodeAttributeSerializer, true);
    }

    public NodeInfo lookup(String clusterId, String serviceId, String nodeId, PresenceObserver<NodeInfo> observer) {
        return lookup(clusterId, serviceId, nodeId, observer, nodeAttributeSerializer, true);
    }

    public NodeInfo lookup(String clusterId, String serviceId, String nodeId, PresenceObserver<NodeInfo> observer,
            NodeAttributeSerializer nodeAttributeSerializer, boolean useCache) {
        /** get node data from zk **/
        String nodePath = getPathScheme().buildRelativePath(clusterId, serviceId, nodeId);
        String path = getPathScheme().getAbsolutePath(PathContext.USER, PathType.PRESENCE, nodePath);

        boolean error = false;

        byte[] bytes = null;
        try {
            PathCacheEntry pathCacheEntry = getPathCache().get(path);
            if (useCache && pathCacheEntry != null) {
                // found in cache
                bytes = pathCacheEntry.getBytes();
            } else {
                // not in cache, so populate from ZK
                Stat stat = new Stat();
                bytes = getZkClient().getData(path, true, stat);

                // update cache
                getPathCache().put(path, stat, bytes, null);
            }

        } catch (KeeperException e) {
            if (e.code() == Code.NONODE) {
                // set up watch on that node
                try {
                    getZkClient().exists(path, true);
                } catch (Exception e1) {
                    logger.error("lookup():  error trying to watch node:  " + e1 + ":  path=" + path, e1);
                }

                logger.debug("lookup():  error trying to fetch node info:  {}:  node does not exist:  path={}",
                        e.getMessage(), path);
            } else {
                logger.error("lookup():  error trying to fetch node info:  " + e, e);
            }
            error = true;
        } catch (Exception e) {
            logger.warn("lookup():  error trying to fetch node info:  " + e, e);
            error = true;
        }

        /** build node info **/
        NodeInfo result = null;
        if (!error) {
            try {
                result = new NodeInfo(clusterId, serviceId, nodeId,
                        bytes != null ? nodeAttributeSerializer.deserialize(bytes) : Collections.EMPTY_MAP);
            } catch (Throwable e) {
                throw new IllegalStateException(
                        "lookup():  error trying to fetch node info:  path=" + path + ":  " + e, e);
            }
        }

        /** add observer if passed in **/
        if (observer != null) {
            this.observerManager.put(path, new PresenceObserverWrapper<NodeInfo>(clusterId, serviceId, nodeId,
                    observer, nodeAttributeSerializer, result));
        }

        return result;
    }

    public void announce(String clusterId, String serviceId, String nodeId) {
        announce(clusterId, serviceId, nodeId, null, null);
    }

    public void announce(String clusterId, String serviceId, String nodeId, Map<String, String> attributeMap) {
        announce(clusterId, serviceId, nodeId, attributeMap, null);
    }

    /**
     * This method only has to be called once per service node and/or when node data changes. Announcements happen
     * asynchronously.
     * 
     * @param clusterId
     * @param serviceId
     * @param nodeId
     * @param attributeMap
     * @param nodeAttributeSerializer
     */
    public void announce(String clusterId, String serviceId, String nodeId, Map<String, String> attributeMap,
            NodeAttributeSerializer nodeAttributeSerializer) {
        // defaults
        if (nodeAttributeSerializer == null) {
            nodeAttributeSerializer = this.getNodeAttributeSerializer();
        }

        // get announcement using path to node
        String nodePath = getPathScheme().buildRelativePath(clusterId, serviceId, nodeId);
        Announcement announcement = this.getAnnouncement(nodePath);
        announcement.setNodeAttributeSerializer(nodeAttributeSerializer);

        // update announcement if node data is different
        NodeInfo nodeInfo = new NodeInfo(clusterId, serviceId, nodeId, attributeMap);
        if (!nodeInfo.equals(announcement.getNodeInfo())) {
            announcement.setNodeInfo(nodeInfo);
            announcement.setLastUpdated(-1);
        }

        // mark as visible
        announcement.setHidden(false);
    }

    public void hide(String clusterId, String serviceId, String nodeId) {
        String nodePath = getPathScheme().buildRelativePath(clusterId, serviceId, nodeId);
        Announcement announcement = this.getAnnouncement(nodePath);
        if (!announcement.isHidden()) {
            announcement.setHidden(true);
            announcement.setLastUpdated(-1);
        }
    }

    public void unhide(String clusterId, String serviceId, String nodeId) {
        String nodePath = getPathScheme().buildRelativePath(clusterId, serviceId, nodeId);
        Announcement announcement = this.getAnnouncement(nodePath);
        if (announcement.isHidden()) {
            announcement.setHidden(false);
            announcement.setLastUpdated(-1);
        }
    }

    Announcement getAnnouncement(String nodePath) {
        // create announcement if necessary
        Announcement announcement = announcementMap.get(nodePath);
        if (announcement == null) {
            Announcement newAnnouncement = new Announcement();
            newAnnouncement.setAclList(getDefaultAclList());

            announcement = announcementMap.putIfAbsent(nodePath, newAnnouncement);
            if (announcement == null) {
                announcement = newAnnouncement;
            }
        }
        return announcement;
    }

    @Override
    public void perform() {
        /** perform revealing and hiding announcements regularly **/
        logger.debug("Processing announcements:  announcementMap.size={}", announcementMap.size());
        Set<String> nodePathSet = announcementMap.keySet();
        long currentTimestamp = System.currentTimeMillis();
        for (String nodePath : nodePathSet) {
            Announcement announcement = announcementMap.get(nodePath);

            // skip if announcement no longer exists or less than heartbeat
            // interval
            if (announcement == null || currentTimestamp - announcement.getLastUpdated() < this.heartbeatIntervalMillis) {
                continue;
            }

            // announce or hide node
            String path = getPathScheme().getAbsolutePath(PathContext.USER, PathType.PRESENCE, nodePath);
            if (announcement.isHidden()) {
                // delete
                try {
                    // delete presence path
                    getZkClient().delete(path, -1);

                    // set up watch for when path comes back if there are
                    // observers
                    if (this.observerManager.isBeingObserved(path)) {
                        getZkClient().exists(path, true);
                    }

                    // set last updated with some randomizer to spread out
                    // requests
                    announcement.setLastUpdated(currentTimestamp
                            + (int) ((Math.random() * heartbeatIntervalMillis / 2)));
                } catch (KeeperException e) {
                    if (e.code() == Code.NONODE) {
                        logger.debug("Node does not exist:  path={}", path);
                    } else {
                        logger.warn("Error trying to remove node:  " + e + ":  path=" + path, e);
                    }
                } catch (InterruptedException e) {
                    logger.warn("hide():  error trying to remove node:  " + e, e);
                }
            } else {
                // do announcement
                try {
                    Map<String, String> attributeMap = announcement.getNodeInfo().getAttributeMap();
                    byte[] leafData = null;
                    if (attributeMap.size() > 0) {
                        leafData = announcement.getNodeAttributeSerializer().serialize(attributeMap);
                    }
                    String pathUpdated = zkUtil.updatePath(getZkClient(), getPathScheme(), path, leafData,
                            announcement.getAclList(), CreateMode.EPHEMERAL, -1);

                    // set last updated with some randomizer to spread out
                    // requests
                    announcement.setLastUpdated(currentTimestamp
                            + (int) ((Math.random() * heartbeatIntervalMillis / 2)));

                    logger.debug("Announced:  path={}", pathUpdated);
                } catch (Exception e) {
                    logger.error("Error while announcing:  " + e + ":  path=" + path, e);
                }
            }// if
        }// for

        /** do zombie node check every 5 minutes **/
        if (System.currentTimeMillis() - lastZombieCheckTimestamp > zombieCheckIntervalMillis) {
            // get exclusive leader lock to perform maintenance duties
            CoordinationService coordinationService = getServiceDirectory().getService("coord");
            DistributedLock adminLock = coordinationService.getLock(PathContext.INTERNAL, getSovereignId(), "presence",
                    "zombie-checker", getDefaultAclList());
            logger.info("Checking for zombie nodes...");
            if (adminLock.tryLock()) {
                try {
                    List<String> clusterIdList = getZkClient().getChildren(
                            getPathScheme().getAbsolutePath(PathContext.USER, PathType.PRESENCE), false);
                    for (String clusterId : clusterIdList) {
                        List<String> serviceIdList = getAvailableServices(clusterId);
                        for (String serviceId : serviceIdList) {
                            // service path
                            String servicePath = getPathScheme().getAbsolutePath(PathContext.USER, PathType.PRESENCE,
                                    clusterId, serviceId);

                            // get children of each service
                            List<String> serviceChildren = getZkClient().getChildren(servicePath, false);

                            // check stat and make sure mtime of each child is
                            // within 4x heartbeatIntervalMillis; if not, delete
                            for (String child : serviceChildren) {
                                String serviceChildPath = getPathScheme().join(servicePath, child);
                                logger.info("Checking for service zombie child nodes:  path={}", servicePath);
                                Stat stat = getZkClient().exists(serviceChildPath, false);
                                long timeDiff = System.currentTimeMillis() - stat.getMtime();
                                if (timeDiff > this.heartbeatIntervalMillis * 4) {
                                    logger.warn("Found zombie node:  deleting:  path={}; millisSinceLastHeartbeat={}",
                                            serviceChildPath, timeDiff);
                                    getZkClient().delete(serviceChildPath, -1);
                                }
                            }// for

                        }// for
                    }// for

                    // update last check timestamp
                    lastZombieCheckTimestamp = System.currentTimeMillis();
                } catch (Exception e) {
                    logger.warn("Error while checking for and removing zombie nodes:  " + e, e);
                } finally {
                    adminLock.unlock();
                    adminLock.destroy();
                }
            }// if tryLock

        }// if
    }

    @Override
    public void init() {

        if (this.getHeartbeatIntervalMillis() < 1000) {
            this.setHeartbeatIntervalMillis(DEFAULT_HEARTBEAT_INTERVAL_MILLIS);
        }

        if (this.getExecutionIntervalMillis() < 1000) {
            this.setExecutionIntervalMillis(DEFAULT_EXECUTION_INTERVAL_MILLIS);
        }
    }

    @Override
    public void destroy() {
        // TODO Auto-generated method stub

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
    public void nodeChildrenChanged(WatchedEvent event) {
        String path = event.getPath();
        PresenceObserverWrapper observerWrapper = observerManager.getObserverWrapperSet(path).iterator().next();
        if (observerWrapper.isService()) {
            // only service nodes can have children
            observerManager.signal(
                    path,
                    lookup(observerWrapper.getClusterId(), observerWrapper.getServiceId(), null,
                            observerWrapper.getNodeAttributeSerializer(), true));
        }
    }

    @Override
    public void nodeCreated(WatchedEvent event) {
        nodeDataChanged(event);
    }

    @Override
    public void nodeDataChanged(WatchedEvent event) {
        String path = event.getPath();
        PresenceObserverWrapper observerWrapper = observerManager.getObserverWrapperSet(path).iterator().next();
        if (observerWrapper.isService()) {
            // call but don't use cache to ensure we re-establish watch
            ServiceInfo updatedValue = lookup(observerWrapper.getClusterId(), observerWrapper.getServiceId(), null,
                    observerWrapper.getNodeAttributeSerializer(), true);
            if (updatedValue != null && !updatedValue.equals(observerWrapper.getCurrentValue())) {
                observerManager.signal(path, updatedValue);
            }
        } else {
            // call but don't use cache to ensure we re-establish watch
            NodeInfo updatedValue = lookup(observerWrapper.getClusterId(), observerWrapper.getServiceId(),
                    observerWrapper.getNodeId(), null, observerWrapper.getNodeAttributeSerializer(), true);
            if (updatedValue != null && !updatedValue.equals(observerWrapper.getCurrentValue())) {
                observerManager.signal(path, updatedValue);
            }
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

    public int getConcurrencyLevel() {
        return concurrencyLevel;
    }

    public void setConcurrencyLevel(int concurrencyLevel) {
        this.concurrencyLevel = concurrencyLevel;
    }

    public int getPathCacheSize() {
        return pathCacheSize;
    }

    public NodeAttributeSerializer getNodeAttributeSerializer() {
        return nodeAttributeSerializer;
    }

    public void setNodeAttributeSerializer(NodeAttributeSerializer nodeAttributeSerializer) {
        this.nodeAttributeSerializer = nodeAttributeSerializer;
    }

    public void setPathCacheSize(int pathCacheSize) {
        this.pathCacheSize = pathCacheSize;
    }

    public long getHeartbeatIntervalMillis() {
        return this.getExecutionIntervalMillis();
    }

    public void setHeartbeatIntervalMillis(int heartbeatIntervalMillis) {
        if (heartbeatIntervalMillis < 1000) {
            throw new IllegalArgumentException("heartbeatIntervalMillis is too short:  heartbeatIntervalMillis="
                    + heartbeatIntervalMillis);
        }
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
    }

    public int getZombieCheckIntervalMillis() {
        return zombieCheckIntervalMillis;
    }

    public void setZombieCheckIntervalMillis(int zombieCheckIntervalMillis) {
        if (zombieCheckIntervalMillis < 1000) {
            throw new IllegalArgumentException("zombieCheckIntervalMillis is too short:  zombieCheckIntervalMillis="
                    + zombieCheckIntervalMillis);
        }
        this.zombieCheckIntervalMillis = zombieCheckIntervalMillis;
    }

    private static class PresenceObserverWrapper<T> extends ServiceObserverWrapper<PresenceObserver<T>> {

        private final String clusterId;
        private final String serviceId;
        private final String nodeId;

        private final NodeAttributeSerializer nodeAttributeSerializer;

        private volatile T currentValue;

        public PresenceObserverWrapper(String clusterId, String serviceId, String nodeId, PresenceObserver<T> observer,
                NodeAttributeSerializer nodeAttributeSerializer, T currentValue) {
            // from super class
            this.observer = observer;

            this.clusterId = clusterId;
            this.serviceId = serviceId;
            this.nodeId = nodeId;
            this.nodeAttributeSerializer = nodeAttributeSerializer;
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

        public String getClusterId() {
            return clusterId;
        }

        public String getServiceId() {
            return serviceId;
        }

        public String getNodeId() {
            return nodeId;
        }

        public NodeAttributeSerializer getNodeAttributeSerializer() {
            return nodeAttributeSerializer;
        }

        public T getCurrentValue() {
            return currentValue;
        }

        public boolean isService() {
            return nodeId == null;
        }

    }

}
