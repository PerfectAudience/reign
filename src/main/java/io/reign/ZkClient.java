package io.reign;

import java.util.List;

import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * Defines the framework's interface to Zookeeper.
 * 
 * @author ypai
 * 
 */
public interface ZkClient {

    public void register(Watcher watcher);

    public void close();

    public Stat exists(final String path, final boolean watch) throws KeeperException, InterruptedException;

    public Stat exists(final String path, Watcher watcher) throws KeeperException, InterruptedException;

    public List<String> getChildren(final String path, final boolean watch, final Stat stat) throws KeeperException,
            InterruptedException;

    public List<String> getChildren(final String path, final Watcher watcher) throws KeeperException,
            InterruptedException;

    public List<String> getChildren(final String path, final boolean watch) throws KeeperException,
            InterruptedException;

    public Stat setData(final String path, final byte[] data, final int version) throws KeeperException,
            InterruptedException;

    public byte[] getData(final String path, final boolean watch, final Stat stat) throws KeeperException,
            InterruptedException;

    public String create(final String path, final byte[] data, final List<ACL> acl, final CreateMode createMode)
            throws KeeperException, InterruptedException;

    public void delete(final String path, final int version) throws InterruptedException, KeeperException;

    public void sync(final String path, final VoidCallback cb, final Object ctx);
}
