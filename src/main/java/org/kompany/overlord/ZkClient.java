package org.kompany.overlord;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * Defines the framework's interface to Zookeeper.
 * 
 * @author ypai
 * 
 */
public interface ZkClient extends Watcher {

    public void process(WatchedEvent event);
}
