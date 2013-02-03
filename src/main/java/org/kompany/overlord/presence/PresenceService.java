package org.kompany.overlord.presence;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.kompany.overlord.AbstractActiveService;
import org.kompany.overlord.PathType;
import org.kompany.overlord.util.PathCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class PresenceService extends AbstractActiveService implements Watcher {

    private static final Logger logger = LoggerFactory.getLogger(PresenceService.class);

    private int pathCacheSize = 250;

    private int concurrencyLevel = 8;

    private PathCache pathCache;

    private NodeAttributeSerializer nodeAttributeSerializer;

    public List<String> getAvailableServices(String clusterId) {
        /** validate args **/
        validatePathFragment(clusterId);

        /** get node data from zk **/
        String path = getPathScheme().getAbsolutePath(PathType.PRESENCE, clusterId);
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
        /** validate args **/
        validatePathFragment(clusterId);
        validatePathFragment(serviceId);

        /** get node data from zk **/
        String path = getPathScheme().getAbsolutePath(PathType.PRESENCE, clusterId);
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
        /** validate args **/
        validatePathFragment(clusterId);
        validatePathFragment(serviceId);
        validatePathFragment(nodeId);

        /** get node data from zk **/
        String path = getPathScheme().getAbsolutePath(PathType.PRESENCE, clusterId);
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

    public void announce(String clusterId, String serviceId, String nodeId, Map<String, Object> attributeMap,
            NodeAttributeSerializer nodeAttributeSerializer) {
        // TODO: implement
    }

    public void hide(String clusterId, String serviceId, String nodeId) {
        /** validate args **/
        validatePathFragment(clusterId);
        validatePathFragment(serviceId);
        validatePathFragment(nodeId);

        /** get node data from zk **/
        String path = getPathScheme().getAbsolutePath(PathType.PRESENCE, clusterId);
        try {
            getZkClient().delete(path, -1);
        } catch (KeeperException e) {
            if (e.code() == Code.NONODE) {
                logger.warn("hide():  error trying to remove node:  " + e + ":  node does not exist:  path={}", path);
            } else {
                logger.warn("hide():  error trying to remove node:  " + e, e);
            }
        } catch (InterruptedException e) {
            logger.warn("hide():  error trying to remove node:  " + e, e);
        }
    }

    @Override
    public void perform() {
        /** announce occasionally **/

        /**
         * check for "zombie" presence nodes that have not been touched recently
         **/
    }

    @Override
    public void init() {
        pathCache = new PathCache(pathCacheSize, concurrencyLevel, getZkClient());
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

        // let cache process event
        pathCache.process(event);

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

    void validatePathFragment(String pathFragment) {
        if (pathFragment.indexOf('/') != -1) {
            throw new IllegalArgumentException(
                    "validatePathFragment():  '/' character is not allowed in path:  pathFragment=" + pathFragment);
        }
    }
}
