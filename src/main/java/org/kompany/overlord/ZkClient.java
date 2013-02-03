package org.kompany.overlord;

import org.apache.zookeeper.Watcher;

/**
 * Defines the framework's interface to Zookeeper.
 * 
 * @author ypai
 * 
 */
public interface ZkClient {

    public void register(Watcher watcher);
}
