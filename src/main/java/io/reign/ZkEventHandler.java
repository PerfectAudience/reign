package io.reign;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * Cleaner interface for handling ZooKeeper events.
 * 
 * @author ypai
 * 
 */
public interface ZkEventHandler extends Watcher {

    public void nodeChildrenChanged(WatchedEvent event);

    public void nodeCreated(WatchedEvent event);

    public void nodeDataChanged(WatchedEvent event);

    public void nodeDeleted(WatchedEvent event);

    public void connected(WatchedEvent event);

    public void disconnected(WatchedEvent event);

    public void sessionExpired(WatchedEvent event);

    /**
     * 
     * @param event
     * @return true if we do not need to process this event
     */
    public boolean filterWatchedEvent(WatchedEvent event);

}
