package org.kompany.overlord.presence;

import java.util.ArrayList;
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
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.kompany.overlord.AbstractActiveService;
import org.kompany.overlord.PathType;
import org.kompany.overlord.util.PathCache;
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

    public static final int DEFAULT_HEARTBEAT_INTERVAL_MILLIS = 5000;

    public static final List<ACL> DEFAULT_ACL_LIST = new ArrayList<ACL>();
    static {
        DEFAULT_ACL_LIST.add(new ACL(ZooDefs.Perms.ALL, new Id("world", "anyone")));
    }

    private int pathCacheSize = 250;

    private int concurrencyLevel = 8;

    private PathCache pathCache;

    private NodeAttributeSerializer nodeAttributeSerializer = new JsonNodeAttributeSerializer();

    private ConcurrentMap<String, Announcement> announcementMap = new ConcurrentHashMap<String, Announcement>();

    private ZkUtil zkUtil = new ZkUtil();

    public List<String> getAvailableServices(String clusterId) {
        /** get node data from zk **/
        String clusterPath = getPathScheme().buildRelativePath(clusterId);
        String path = getPathScheme().getAbsolutePath(PathType.PRESENCE, clusterPath);
        List<String> children = null;
        try {
            Stat stat = new Stat();
            children = getZkClient().getChildren(path, true, stat);

            pathCache.put(path, stat, null, children);

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

    public ServiceInfo lookup(String clusterId, String serviceId, NodeAttributeSerializer nodeAttributeSerializer) {
        /** get node data from zk **/
        String servicePath = getPathScheme().buildRelativePath(clusterId, serviceId);
        String path = getPathScheme().getAbsolutePath(PathType.PRESENCE, servicePath);
        List<String> children = null;
        try {
            Stat stat = new Stat();
            children = getZkClient().getChildren(path, true, stat);

            pathCache.put(path, stat, null, children);

        } catch (KeeperException e) {
            if (e.code() == Code.NONODE) {
                logger.warn(
                        "lookup():  error trying to fetch service info:  " + e + ":  node does not exist:  path={}",
                        path);
            } else {
                logger.warn("lookup():  error trying to fetch service info:  " + e, e);
            }
            return null;
        } catch (InterruptedException e) {
            logger.warn("lookup():  error trying to fetch service info:  " + e, e);
            return null;
        }

        /** build service info **/
        ServiceInfo result = null;
        if (children != null) {
            result = new ServiceInfo();
            result.setClusterId(clusterId);
            result.setServiceId(serviceId);
            result.setNodeIdList(children);
        }

        return result;

    }

    public NodeInfo lookup(String clusterId, String serviceId, String nodeId,
            NodeAttributeSerializer nodeAttributeSerializer) {
        /** get node data from zk **/
        String nodePath = getPathScheme().buildRelativePath(clusterId, serviceId, nodeId);
        String path = getPathScheme().getAbsolutePath(PathType.PRESENCE, nodePath);
        byte[] bytes = null;
        try {
            Stat stat = new Stat();
            bytes = getZkClient().getData(path, true, stat);

            pathCache.put(path, stat, bytes, null);

        } catch (KeeperException e) {
            if (e.code() == Code.NONODE) {
                logger.warn("lookup():  error trying to fetch node info:  " + e + ":  node does not exist:  path={}",
                        path);
            } else {
                logger.warn("lookup():  error trying to fetch node info:  " + e, e);
            }
            return null;
        } catch (InterruptedException e) {
            logger.warn("lookup():  error trying to fetch node info:  " + e, e);
            return null;
        }

        /** build node info **/
        NodeInfo result = null;
        try {
            result = new NodeInfo(clusterId, serviceId, nodeId,
                    bytes != null ? nodeAttributeSerializer.deserialize(bytes) : Collections.EMPTY_MAP);
        } catch (Throwable e) {
            throw new IllegalStateException("lookup():  error trying to fetch node info:  path=" + path + ":  " + e, e);
        }

        return result;
    }

    public void announce(String clusterId, String serviceId, String nodeId) {
        announce(clusterId, serviceId, nodeId, null, null);
    }

    public void announce(String clusterId, String serviceId, String nodeId, Map<String, Object> attributeMap) {
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
    public void announce(String clusterId, String serviceId, String nodeId, Map<String, Object> attributeMap,
            NodeAttributeSerializer nodeAttributeSerializer) {
        // defaults
        if (nodeAttributeSerializer == null) {
            nodeAttributeSerializer = this.getNodeAttributeSerializer();
        }

        // get announcement using path to node
        String nodePath = getPathScheme().buildRelativePath(clusterId, serviceId, nodeId);
        Announcement announcement = this.getAnnouncement(nodePath);
        announcement.setNodeAttributeSerializer(nodeAttributeSerializer);

        // update announcement
        NodeInfo nodeInfo = new NodeInfo(clusterId, serviceId, nodeId, attributeMap);
        announcement.setNodeInfo(nodeInfo);

        // mark as visible
        announcement.setHidden(false);
    }

    public void hide(String clusterId, String serviceId, String nodeId) {
        String nodePath = getPathScheme().buildRelativePath(clusterId, serviceId, nodeId);
        Announcement announcement = this.getAnnouncement(nodePath);
        announcement.setHidden(true);
    }

    public void unhide(String clusterId, String serviceId, String nodeId) {
        String nodePath = getPathScheme().buildRelativePath(clusterId, serviceId, nodeId);
        Announcement announcement = this.getAnnouncement(nodePath);
        announcement.setHidden(false);
    }

    Announcement getAnnouncement(String nodePath) {
        // create announcement if necessary
        Announcement announcement = announcementMap.get(nodePath);
        if (announcement == null) {
            Announcement newAnnouncement = new Announcement();
            newAnnouncement.setAclList(DEFAULT_ACL_LIST);

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
        for (String nodePath : nodePathSet) {
            Announcement announcement = announcementMap.get(nodePath);

            // skip if announcement no longer exists
            if (announcement == null) {
                continue;
            }

            // announce or hide node
            String path = getPathScheme().getAbsolutePath(PathType.PRESENCE, nodePath);
            if (announcement.isHidden()) {
                // delete
                try {
                    getZkClient().delete(path, -1);
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
                // TODO: do announcement
                try {
                    byte[] leafData = announcement.getNodeAttributeSerializer().serialize(
                            announcement.getNodeInfo().getAttributeMap());
                    String pathUpdated = zkUtil.updatePath(getZkClient(), getPathScheme(), path, leafData,
                            announcement.getAclList(), CreateMode.EPHEMERAL, -1);

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
        pathCache = new PathCache(pathCacheSize, concurrencyLevel, getZkClient());

        if (this.getHeartbeatIntervalMillis() < 1000) {
            this.setHeartbeatIntervalMillis(DEFAULT_HEARTBEAT_INTERVAL_MILLIS);
        }
    }

    @Override
    public void destroy() {
        // TODO Auto-generated method stub

    }

    @Override
    public void process(WatchedEvent event) {
        // log if >DEBUG
        if (logger.isDebugEnabled()) {
            logger.debug("***** Received ZooKeeper Event:\n"
                    + ReflectionToStringBuilder.toString(event, ToStringStyle.DEFAULT_STYLE));

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

    public void setHeartbeatIntervalMillis(long heartbeatIntervalMillis) {
        if (heartbeatIntervalMillis < 100) {
            throw new IllegalArgumentException("heartbeatIntervalMillis is too short:  heartbeatIntervalMillis="
                    + heartbeatIntervalMillis);
        }
        this.setExecutionIntervalMillis(heartbeatIntervalMillis);
    }

}
