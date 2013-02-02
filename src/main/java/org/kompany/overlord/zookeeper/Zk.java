package org.kompany.overlord.zookeeper;

import java.util.List;

import org.apache.zookeeper.AsyncCallback.ACLCallback;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public interface Zk {

    public void addAuthInfo(String scheme, byte[] auth);

    public void close() throws InterruptedException;

    public void create(String path, byte[] data, List<ACL> acl,
            CreateMode createMode, StringCallback cb, Object ctx);

    public String create(String path, byte[] data, List<ACL> acl,
            CreateMode createMode) throws KeeperException, InterruptedException;

    public void delete(String arg0, int arg1, VoidCallback arg2, Object arg3);

    public void delete(String arg0, int arg1) throws InterruptedException,
            KeeperException;

    public void exists(String path, boolean watch, StatCallback cb, Object ctx);

    public Stat exists(String path, boolean watch) throws KeeperException,
            InterruptedException;

    public void exists(String path, Watcher watcher, StatCallback cb, Object ctx);

    public Stat exists(String path, Watcher watcher) throws KeeperException,
            InterruptedException;

    public void getACL(String path, Stat stat, ACLCallback cb, Object ctx);

    public List<ACL> getACL(String path, Stat stat) throws KeeperException,
            InterruptedException;

    public void getChildren(String path, boolean watch, Children2Callback cb,
            Object ctx);

    public void getChildren(String path, boolean watch, ChildrenCallback cb,
            Object ctx);

    public List<String> getChildren(String path, boolean watch, Stat stat)
            throws KeeperException, InterruptedException;

    public List<String> getChildren(String path, boolean watch)
            throws KeeperException, InterruptedException;

    public void getChildren(String path, Watcher watcher, Children2Callback cb,
            Object ctx);

    public void getChildren(String path, Watcher watcher, ChildrenCallback cb,
            Object ctx);

    public List<String> getChildren(String path, Watcher watcher, Stat stat)
            throws KeeperException, InterruptedException;

    public List<String> getChildren(String path, Watcher watcher)
            throws KeeperException, InterruptedException;

    public void getData(String path, boolean watch, DataCallback cb, Object ctx);

    public byte[] getData(String path, boolean watch, Stat stat)
            throws KeeperException, InterruptedException;

    public void getData(String path, Watcher watcher, DataCallback cb,
            Object ctx);

    public byte[] getData(String path, Watcher watcher, Stat stat)
            throws KeeperException, InterruptedException;

    public long getSessionId();

    public byte[] getSessionPasswd();

    public int getSessionTimeout();

    public States getState();

    public void register(Watcher watcher);

    public void setACL(String path, List<ACL> acl, int version,
            StatCallback cb, Object ctx);

    public Stat setACL(String path, List<ACL> acl, int version)
            throws KeeperException, InterruptedException;

    public void setData(String path, byte[] data, int version, StatCallback cb,
            Object ctx);

    public Stat setData(String path, byte[] data, int version)
            throws KeeperException, InterruptedException;

    public void sync(String path, VoidCallback cb, Object ctx);

    @Override
    public String toString();
}
