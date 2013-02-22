package org.kompany.overlord;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.zookeeper.WatchedEvent;
import org.kompany.overlord.util.PathCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public abstract class AbstractService implements Service, ZkEventHandler {

    private static final Logger logger = LoggerFactory.getLogger(AbstractService.class);

    private PathScheme pathScheme;
    private ZkClient zkClient;

    private PathCache pathCache;

    private ServiceDirectory serviceDirectory;

    public ServiceDirectory getServiceDirectory() {
        return serviceDirectory;
    }

    @Override
    public void setServiceDirectory(ServiceDirectory serviceDirectory) {
        this.serviceDirectory = serviceDirectory;
    }

    public PathCache getPathCache() {
        return pathCache;
    }

    @Override
    public void setPathCache(PathCache pathCache) {
        this.pathCache = pathCache;
    }

    public PathScheme getPathScheme() {
        return pathScheme;
    }

    @Override
    public void setPathScheme(PathScheme pathScheme) {
        this.pathScheme = pathScheme;

    }

    public ZkClient getZkClient() {
        return zkClient;
    }

    @Override
    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    @Override
    public abstract void init();

    @Override
    public abstract void destroy();

    @Override
    public boolean filterWatchedEvent(WatchedEvent event) {
        return false;
    }

    @Override
    public void nodeChildrenChanged(WatchedEvent event) {
    }

    @Override
    public void nodeCreated(WatchedEvent event) {
    }

    @Override
    public void nodeDataChanged(WatchedEvent event) {
    }

    @Override
    public void nodeDeleted(WatchedEvent event) {
    }

    @Override
    public void connected(WatchedEvent event) {
    }

    @Override
    public void disconnected(WatchedEvent event) {
    }

    @Override
    public void sessionExpired(WatchedEvent event) {
    }

    @Override
    public void process(WatchedEvent event) {
        /** log event **/
        // log if DEBUG
        if (logger.isDebugEnabled()) {
            logger.debug("***** Received ZooKeeper Event:  {}",
                    ReflectionToStringBuilder.toString(event, ToStringStyle.DEFAULT_STYLE));

        }

        /** check if we are filtering this event **/
        if (filterWatchedEvent(event)) {
            return;
        }

        /** process events **/
        switch (event.getType()) {
        case NodeChildrenChanged:
            nodeChildrenChanged(event);
            break;
        case NodeCreated:
            nodeCreated(event);
            break;
        case NodeDataChanged:
            nodeDataChanged(event);
            break;
        case NodeDeleted:
            nodeDeleted(event);
            break;
        case None:
            Event.KeeperState eventState = event.getState();
            if (eventState == Event.KeeperState.SyncConnected) {
                connected(event);
            } else if (eventState == Event.KeeperState.Disconnected) {
                disconnected(event);
            } else if (eventState == Event.KeeperState.Expired) {
                sessionExpired(event);
            } else {
                logger.warn("Unhandled event state:  eventType={}; eventState={}", event.getType(), event.getState());
            }
            break;
        default:
            logger.warn("Unhandled event type:  eventType={}; eventState={}", event.getType(), event.getState());
        }
    }

}
