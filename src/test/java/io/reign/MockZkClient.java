/*
 Copyright 2013 Yen Pai ypai@reign.io

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package io.reign;

import io.reign.zk.ResilientZooKeeper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mock ZkClient that mimics ZooKeeper functionality for unit tests.
 * 
 * Additional detail about ZK watches from http://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#ch_zkWatches:
 * 
 * This refers to the different ways a node can change. It helps to think of ZooKeeper as maintaining two lists of
 * watches: data watches and child watches. getData() and exists() set data watches. getChildren() sets child watches.
 * Alternatively, it may help to think of watches being set according to the kind of data returned. getData() and
 * exists() return information about the data of the node, whereas getChildren() returns a list of children. Thus,
 * setData() will trigger data watches for the znode being set (assuming the set is successful). A successful create()
 * will trigger a data watch for the znode being created and a child watch for the parent znode. A successful delete()
 * will trigger both a data watch and a child watch (since there can be no more children) for a znode being deleted as
 * well as a child watch for the parent znode.
 * 
 * @author ypai
 * 
 */
public class MockZkClient implements ZkClient {

    private static final Logger logger = LoggerFactory.getLogger(MockZkClient.class);

    private static final Pattern PATTERN_PATH_TOKENIZER = Pattern.compile("/");

    /** Map of String (path) --> Watcher */
    private final ConcurrentMap<String, Set<Watcher>> dataWatcherMap = new ConcurrentHashMap<String, Set<Watcher>>(64,
            0.9f, 2);

    /** Map of String (path) --> Watcher */
    private final ConcurrentMap<String, Set<Watcher>> childWatcherMap = new ConcurrentHashMap<String, Set<Watcher>>(64,
            0.9f, 2);

    void addDataWatcher(String path, Watcher watcher) {
        logger.debug("Adding data watcher:  path={}", path);

        Set<Watcher> watcherSet = dataWatcherMap.get(path);
        if (watcherSet == null) {
            Set<Watcher> newWatcherSet = Collections.newSetFromMap(new ConcurrentHashMap<Watcher, Boolean>(8, 0.9f, 1));
            watcherSet = dataWatcherMap.putIfAbsent(path, newWatcherSet);
            if (watcherSet == null) {
                watcherSet = newWatcherSet;
            }
        }
        watcherSet.add(watcher);
    }

    Set<Watcher> getDataWatcherSet(String path) {
        Set<Watcher> result = dataWatcherMap.get(path);
        if (result != null) {
            return result;
        } else {
            return Collections.EMPTY_SET;
        }
    }

    void addChildWatcher(String path, Watcher watcher) {
        logger.debug("Adding child watcher:  path={}", path);

        Set<Watcher> watcherSet = childWatcherMap.get(path);
        if (watcherSet == null) {
            Set<Watcher> newWatcherSet = Collections.newSetFromMap(new ConcurrentHashMap<Watcher, Boolean>(8, 0.9f, 1));
            watcherSet = childWatcherMap.putIfAbsent(path, newWatcherSet);
            if (watcherSet == null) {
                watcherSet = newWatcherSet;
            }
        }
        watcherSet.add(watcher);
    }

    Set<Watcher> getChildWatcherSet(String path) {
        Set<Watcher> result = childWatcherMap.get(path);
        if (result != null) {
            return result;
        } else {
            return Collections.EMPTY_SET;
        }
    }

    public static class MockZkNode {
        private byte[] data;
        private Stat stat = new Stat();
        private final Map<String, MockZkNode> children = new HashMap<String, MockZkNode>(8);
        private final AtomicInteger sequence = new AtomicInteger(0);
        private final MockZkNode parent;
        private final String name;
        private CreateMode createMode;

        public MockZkNode(MockZkNode parent, String name, byte[] data, CreateMode createMode) {
            this.parent = parent;
            this.name = name;
            this.data = data;
            this.createMode = createMode;
            stat.setCtime(System.currentTimeMillis());
        }

        public CreateMode getCreateMode() {
            return createMode;
        }

        public void setCreateMode(CreateMode createMode) {
            this.createMode = createMode;
        }

        public int getNextSequenceId() {
            return sequence.getAndIncrement();
        }

        public MockZkNode getParent() {
            return parent;
        }

        public String getName() {
            return name;
        }

        public synchronized byte[] getData() {
            return data;
        }

        public synchronized void setData(byte[] data) {
            this.data = data;
            stat.setMtime(System.currentTimeMillis());
        }

        public synchronized Stat getStat() {
            stat.setDataLength(data != null ? data.length : 0);
            return stat;
        }

        public synchronized void setStat(Stat stat) {
            this.stat = stat;
        }

        public synchronized List<String> getChildList() {
            List<String> childList = new ArrayList<String>(children.size());
            for (String key : children.keySet()) {
                childList.add(key);
            }
            return childList;
        }

        public synchronized int getChildCount() {
            return this.children.size();
        }

        public synchronized void putChild(MockZkNode node) {
            this.children.put(node.getName(), node);
            stat.setNumChildren(this.getChildCount());
        }

        public synchronized MockZkNode getChild(String name) {
            return this.children.get(name);
        }

        public synchronized MockZkNode removeChild(String name) {
            MockZkNode removed = this.children.remove(name);
            stat.setNumChildren(this.getChildCount());
            return removed;
        }

    }

    private final MockZkNode rootNode = new MockZkNode(null, "", null, CreateMode.PERSISTENT);

    private static class DefaultWatcher implements Watcher {
        private final Set<Watcher> defaultWatcherSet = Collections
                .newSetFromMap(new ConcurrentHashMap<Watcher, Boolean>(8, 0.9f, 1));

        @Override
        public void process(WatchedEvent event) {
            for (Watcher watcher : defaultWatcherSet) {
                logger.debug("Notifying registered watcher:  {}",
                        ReflectionToStringBuilder.toString(event, ToStringStyle.DEFAULT_STYLE));
                watcher.process(event);
            }
        }

        public void register(Watcher watcher) {
            this.defaultWatcherSet.add(watcher);
        }
    }

    private final DefaultWatcher defaultWatcher = new DefaultWatcher();

    // @Override
    // public void process(WatchedEvent event) {
    // if (logger.isDebugEnabled()) {
    // logger.debug("***** Received ZooKeeper Event:  {}",
    // ReflectionToStringBuilder.toString(event, ToStringStyle.DEFAULT_STYLE));
    //
    // }
    //
    // // for (Watcher watcher : defaultWatcherSet) {
    // // watcher.process(event);
    // // }
    // }

    private void dataWatchNode(String path, Watcher watcher) {
        // MockZkNode node = findNode(path);
        addDataWatcher(path, watcher);
    }

    /**
     * TODO: check behavior if we try to add child watch to node that doesn't exist
     * 
     * @param path
     * @param watcher
     */
    private void childWatchNode(String path, Watcher watcher) {
        MockZkNode node = findNode(path);
        if (node != null) {
            addChildWatcher(path, watcher);
        }
    }

    void emitWatchedEvent(String path, EventType eventType, KeeperState keeperState) {
        logger.debug("Emitting event:  path={}; eventType={}; keeperState={}", new Object[] { path, eventType,
                keeperState });

        WatchedEvent event = new WatchedEvent(eventType, keeperState, path);

        ArrayList<Watcher> watchersToRemove = new ArrayList<Watcher>();
        if (eventType == EventType.NodeDataChanged || eventType == EventType.NodeCreated
                || eventType == EventType.NodeDeleted) {

            // notify watchers on that node
            for (Watcher watcher : getDataWatcherSet(path)) {
                logger.debug("Notifying data watcher:  path={}; eventType={}; keeperState={}", new Object[] { path,
                        eventType, keeperState });
                watcher.process(event);
                watchersToRemove.add(watcher);
            }

            // remove watches after single use, like real ZK behavior
            if (watchersToRemove.size() > 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Removing {} data watchers after emitting event:  path={}", watchersToRemove.size(),
                            path);
                }
                for (Watcher watcher : watchersToRemove) {
                    getDataWatcherSet(path).remove(watcher);
                }
            }

            // notify parents if necessary for child watches
            if (eventType == EventType.NodeCreated || eventType == EventType.NodeDeleted) {
                // ArrayList<Watcher> childWatchersToRemove = new ArrayList<Watcher>();

                String parentPath = getParentPath(path);
                emitWatchedEvent(parentPath, EventType.NodeChildrenChanged, keeperState);

                // for (Watcher watcher : getChildWatcherSet(parentPath)) {
                // logger.debug("Notifying child watcher:  parentPath={}; childPath={}; eventType={}; keeperState={}",
                // new Object[] { parentPath, path, eventType, keeperState });
                // watcher.process(event);
                // childWatchersToRemove.add(watcher);
                // }
                //
                // // remove watches after single use, like real ZK behavior
                // if (childWatchersToRemove.size() > 0) {
                // if (logger.isDebugEnabled()) {
                // logger.debug("Removing {} child watchers after emitting event:  parentPath={}",
                // childWatchersToRemove.size(), parentPath);
                // }
                // for (Watcher watcher : childWatchersToRemove) {
                // getChildWatcherSet(parentPath).remove(watcher);
                // }
                // }
            }// if

        } else if (eventType == EventType.NodeChildrenChanged) {

            // notify watchers on that node
            for (Watcher watcher : getChildWatcherSet(path)) {
                logger.debug("Notifying child watcher:  path={}; eventType={}; keeperState={}", new Object[] { path,
                        eventType, keeperState });
                watcher.process(event);
                watchersToRemove.add(watcher);
            }

            // remove watches after single use, like real ZK behavior
            if (watchersToRemove.size() > 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Removing {} child watchers after emitting event:  path={}", watchersToRemove.size(),
                            path);
                }
                for (Watcher watcher : watchersToRemove) {
                    getChildWatcherSet(path).remove(watcher);
                }
            }
        }

        // alert all watchers for this event type
        if (eventType == EventType.None) {
            // for (Watcher watcher : defaultWatcherSet) {
            // watcher.process(event);
            // }
            defaultWatcher.process(event);
        }
    }

    String getParentPath(String path) {
        if ("/".equals(path)) {
            return null;
        }
        return path.substring(0, path.lastIndexOf("/"));
    }

    MockZkNode findNode(String path) {
        return findNode(path, 0);
    }

    /**
     * 
     * @param path
     * @param depthTrimFromLeafLevel
     *            -1 to skip 1 from the end, 0 to search to leaf level
     * @return
     */
    MockZkNode findNode(String path, int depthTrimFromLeafLevel) {
        if (path == null) {
            return null;
        }

        if (!path.startsWith("/")) {
            throw new IllegalArgumentException("Paths must start with /");
        }

        String[] tokens = PATTERN_PATH_TOKENIZER.split(path);
        MockZkNode currentNode = rootNode;
        for (int i = 0; i < tokens.length + depthTrimFromLeafLevel; i++) {
            String token = tokens[i];

            if (token != null && !token.isEmpty()) {
                currentNode = currentNode.getChild(token);
            }

            if (currentNode == null) {
                logger.trace("Did not find node:  token='{}'", token);
                return null;
            } else {
                logger.trace("Found node:  token='{}'", token);
            }
        }
        return currentNode;
    }

    @Override
    public void sync(String path, VoidCallback cb, Object ctx) {
        cb.processResult(0, path, ctx);

    }

    /**
     * @param path
     * @return true if we get it
     */
    private boolean deleteNode(String path) throws KeeperException {
        String[] tokens = PATTERN_PATH_TOKENIZER.split(path);
        MockZkNode currentNode = rootNode;
        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i];
            if (token != null && !token.isEmpty()) {
                if (i == tokens.length - 1) {
                    MockZkNode nodeToDelete = currentNode.getChild(token);
                    if (nodeToDelete == null) {
                        throw new KeeperException.NoNodeException();
                    }

                    if (nodeToDelete.getChildCount() > 0) {
                        throw new KeeperException.NotEmptyException();
                    }

                    // emit event for node change
                    this.emitWatchedEvent(path, EventType.NodeDeleted, KeeperState.SyncConnected);

                    // emit event for parent node children change
                    int lastSlashIndex = path.lastIndexOf("/");
                    if (lastSlashIndex == 0) {
                        this.emitWatchedEvent("/", EventType.NodeChildrenChanged, KeeperState.SyncConnected);
                    } else {
                        this.emitWatchedEvent(path.substring(0, lastSlashIndex), EventType.NodeChildrenChanged,
                                KeeperState.SyncConnected);
                    }

                    return currentNode.removeChild(token) != null;
                }
                currentNode = currentNode.getChild(token);
            }
            if (currentNode == null) {
                return false;
            }
        }
        return false;
    }

    @Override
    public Stat exists(final String path, Watcher watcher) throws KeeperException, InterruptedException {
        if (watcher != null) {
            dataWatchNode(path, watcher);
        }
        MockZkNode node = findNode(path);
        return node != null ? node.getStat() : null;
    }

    @Override
    public void register(Watcher watcher) {
        logger.debug("register():  adding watcher:  " + watcher);
        defaultWatcher.register(watcher);
    }

    @Override
    public void close() {
    }

    @Override
    public Stat exists(String path, boolean watch) throws KeeperException, InterruptedException {
        if (watch) {
            dataWatchNode(path, defaultWatcher);
        }
        MockZkNode node = findNode(path);
        return node != null ? node.getStat() : null;
    }

    @Override
    public List<String> getChildren(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
        if (watch) {
            childWatchNode(path, defaultWatcher);
        }
        MockZkNode node = findNode(path);
        if (node != null) {
            copyStat(node.getStat(), stat);
        }
        return node != null ? node.getChildList() : null;
    }

    @Override
    public List<String> getChildren(String path, Watcher watcher) throws KeeperException, InterruptedException {
        if (watcher != null) {
            childWatchNode(path, watcher);
        }
        MockZkNode node = findNode(path);
        return node != null ? node.getChildList() : null;
    }

    @Override
    public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException {
        if (watch) {
            childWatchNode(path, defaultWatcher);
        }
        MockZkNode node = findNode(path);
        return node != null ? node.getChildList() : null;
    }

    @Override
    public Stat setData(String path, byte[] data, int version) throws KeeperException, InterruptedException {
        MockZkNode node = findNode(path);
        if (node != null) {

            // if data differs from current data, fire event for watchers
            if (!Arrays.equals(data, node.getData())) {
                this.emitWatchedEvent(path, EventType.NodeDataChanged, KeeperState.SyncConnected);
            }

            node.setData(data);

            return node.getStat();
        } else {
            throw new KeeperException.NoNodeException();
        }
    }

    @Override
    public byte[] getData(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
        if (watch) {
            dataWatchNode(path, defaultWatcher);
        }

        MockZkNode node = findNode(path);
        if (node != null) {
            copyStat(node.getStat(), stat);

            return node.getData();
        }
        return null;
    }

    void copyStat(Stat src, Stat dest) {
        dest.setAversion(src.getAversion());
        dest.setCtime(src.getCtime());
        dest.setMtime(src.getMtime());
        dest.setCversion(src.getCversion());
        dest.setMzxid(src.getMzxid());
        dest.setNumChildren(src.getNumChildren());
    }

    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException,
            InterruptedException {

        logger.debug("Creating node:  path={}", path);

        // no parent
        MockZkNode parentNode = findNode(path, -1);
        if (parentNode == null) {
            throw new KeeperException.NoNodeException("Parent for path '" + path + "' does not exist!");
        }

        // already exists
        MockZkNode nodeToCreate = findNode(path);
        if (nodeToCreate != null) {
            throw new KeeperException.NodeExistsException(path);
        }

        parentNode.putChild(new MockZkNode(parentNode, pathLeaf(path), data, createMode));

        this.emitWatchedEvent(path, EventType.NodeCreated, KeeperState.SyncConnected);

        return path;
    }

    @Override
    public void delete(String path, int version) throws InterruptedException, KeeperException {
        deleteNode(path);
    }

    String pathLeaf(String path) {
        int lastSlashIndex = path.lastIndexOf("/");
        if (lastSlashIndex != -1) {
            return path.substring(lastSlashIndex + 1);
        }
        return null;
    }

    // public static void main(String[] args) throws Exception {
    // ZkClient zkClient = new ResilientZooKeeper("localhost:2181", 15000);
    // zkClient.delete("/test", -1);
    // zkClient.close();
    // zkClient.exists("/test", false);
    //
    // // ZooKeeper zkClient = new ZooKeeper("localhost:2181", 15000, null);
    // // zkClient.exists("/test", false);
    // // zkClient.close();
    // // zkClient.exists("/test", false);
    //
    // }
}
