package org.kompany.overlord;

import org.apache.zookeeper.WatchedEvent;

public interface ZkEventHandler {
    /**
     * Handle Zookeeper event.
     * 
     * @param event
     */
    public void handle(WatchedEvent event);
}
