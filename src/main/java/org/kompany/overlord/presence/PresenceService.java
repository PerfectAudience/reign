package org.kompany.overlord.presence;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.kompany.overlord.AbstractActiveService;
import org.kompany.overlord.PathType;
import org.kompany.overlord.Sovereign;
import org.kompany.overlord.util.PathCache.PathCacheEntry;
import org.kompany.overlord.util.ZkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class PresenceService extends AbstractActiveService implements Watcher {

    private static final Logger logger = LoggerFactory.getLogger(PresenceService.class);

    public static final int DEFAULT_HEARTBEAT_INTERVAL_MILLIS = 30000;

    public static final int DEFAULT_EXECUTION_INTERVAL_MILLIS = 2000;

    private int pathCacheSize = 250;

    private int concurrencyLevel = 8;

    private int heartbeatIntervalMillis = DEFAULT_HEARTBEAT_INTERVAL_MILLIS;

    private NodeAttributeSerializer nodeAttributeSerializer = new JsonNodeAttributeSerializer();

    private ConcurrentMap<String, Announcement> announcementMap = new ConcurrentHashMap<String, Announcement>();

    private ConcurrentMap<String, PresenceObserverWrapper> observerMap = new ConcurrentHashMap<String, PresenceObserverWrapper>();

    private ZkUtil zkUtil = new ZkUtil();

    public List<String> getAvailableServices(String clusterId) {
        /** get node data from zk **/
        String clusterPath = getPathScheme().buildRelativePath(clusterId);
        String path = getPathScheme().getAbsolutePath(PathType.PRESENCE, clusterPath);
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
        String path = getPathScheme().getAbsolutePath(PathType.PRESENCE, servicePath);

        boolean error = false;

        List<String> children = null;
        try {
            PathCacheEntry pathCacheEntry = getPathCache().get(path);
            if (useCache && pathCacheEntry != null) {
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
            this.observerMap.put(path, new PresenceObserverWrapper<ServiceInfo>(clusterId, serviceId, null, observer,
                    nodeAttributeSerializer, result));
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
        String path = getPathScheme().getAbsolutePath(PathType.PRESENCE, nodePath);

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
            this.observerMap.put(path, new PresenceObserverWrapper<NodeInfo>(clusterId, serviceId, nodeId, observer,
                    nodeAttributeSerializer, result));
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
            newAnnouncement.setAclList(Sovereign.DEFAULT_ACL_LIST);

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

            // skip if announcement no longer exists or less than heartbeat interval
            if (announcement == null || currentTimestamp - announcement.getLastUpdated() < this.heartbeatIntervalMillis) {
                continue;
            }

            // announce or hide node
            String path = getPathScheme().getAbsolutePath(PathType.PRESENCE, nodePath);
            if (announcement.isHidden()) {
                // delete
                try {
                    getZkClient().delete(path, -1);

                    // set last updated with some randomizer to spread out requests
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

                    // set last updated with some randomizer to spread out requests
                    announcement.setLastUpdated(currentTimestamp
                            + (int) ((Math.random() * heartbeatIntervalMillis / 2)));

                    logger.debug("Announced:  path={}", pathUpdated);
                } catch (Exception e) {
                    logger.error("Error while announcing:  " + e + ":  path=" + path, e);
                }
            }// if
        }// for

        // TODO: zombie node check?
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
    public void process(WatchedEvent event) {
        // log if DEBUG
        if (logger.isDebugEnabled()) {
            logger.debug("***** Received ZooKeeper Event:  {}",
                    ReflectionToStringBuilder.toString(event, ToStringStyle.DEFAULT_STYLE));

        }

        // check observer map
        String path = event.getPath();
        PresenceObserverWrapper observerWrapper = observerMap.get(path);
        if (observerWrapper != null) {
            switch (event.getType()) {
            case NodeChildrenChanged:
                if (observerWrapper.isService()) {
                    // only service nodes can have children
                    observerWrapper.getObserver().handle(
                            lookup(observerWrapper.getClusterId(), observerWrapper.getServiceId(),
                                    observerWrapper.getObserver(), observerWrapper.getNodeAttributeSerializer(), true));
                }
                break;
            case NodeCreated:
                if (observerWrapper.isService()) {
                    observerWrapper.getObserver().handle(
                            lookup(observerWrapper.getClusterId(), observerWrapper.getServiceId(),
                                    observerWrapper.getObserver(), observerWrapper.getNodeAttributeSerializer(), true));
                } else {
                    observerWrapper.getObserver().handle(
                            lookup(observerWrapper.getClusterId(), observerWrapper.getServiceId(),
                                    observerWrapper.getNodeId(), observerWrapper.getObserver(),
                                    observerWrapper.getNodeAttributeSerializer(), true));
                }
                break;
            case NodeDataChanged:
                if (observerWrapper.isService()) {
                    ServiceInfo updatedValue = lookup(observerWrapper.getClusterId(), observerWrapper.getServiceId(),
                            observerWrapper.getObserver(), observerWrapper.getNodeAttributeSerializer(), true);
                    if (updatedValue != null && !updatedValue.equals(observerWrapper.getCurrentValue())) {
                        observerWrapper.getObserver().handle(updatedValue);
                    }
                } else {
                    NodeInfo updatedValue = lookup(observerWrapper.getClusterId(), observerWrapper.getServiceId(),
                            observerWrapper.getNodeId(), observerWrapper.getObserver(),
                            observerWrapper.getNodeAttributeSerializer(), true);
                    if (updatedValue != null && !updatedValue.equals(observerWrapper.getCurrentValue())) {
                        observerWrapper.getObserver().handle(updatedValue);
                    }
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

    private static class PresenceObserverWrapper<T> {
        private PresenceObserver<T> observer;

        private String clusterId;
        private String serviceId;
        private String nodeId;

        private NodeAttributeSerializer nodeAttributeSerializer;

        private T currentValue;

        public PresenceObserverWrapper(String clusterId, String serviceId, String nodeId, PresenceObserver<T> observer,
                NodeAttributeSerializer nodeAttributeSerializer, T currentValue) {
            this.clusterId = clusterId;
            this.serviceId = serviceId;
            this.nodeId = nodeId;
            this.observer = observer;
            this.nodeAttributeSerializer = nodeAttributeSerializer;
            this.currentValue = currentValue;
        }

        public PresenceObserver<T> getObserver() {
            return observer;
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
