package org.kompany.overlord;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

/**
 * Defines the framework's interface to Zookeeper.
 * 
 * @author ypai
 * 
 */
public interface ZkClient {

    public void register(Watcher watcher);

    public List<String> getChildren(final String path, final boolean watch, final Stat stat) throws KeeperException,
            InterruptedException;

    public byte[] getData(final String path, final boolean watch, final Stat stat) throws KeeperException,
            InterruptedException;

    public void delete(final String path, final int version) throws InterruptedException, KeeperException;
}
