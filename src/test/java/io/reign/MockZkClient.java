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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

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
 * @author ypai
 * 
 */
public class MockZkClient implements ZkClient, Watcher {

    private static final Logger logger = LoggerFactory.getLogger(MockZkClient.class);

    private static final Pattern PATTERN_PATH_TOKENIZER = Pattern.compile("/");

    public static class MockZkNode {
        private byte[] data;
        private Stat stat = new Stat();
        private final Map<String, MockZkNode> children = new HashMap<String, MockZkNode>(8);
        private final AtomicInteger sequence = new AtomicInteger(0);
        private final MockZkNode parent;
        private final String name;
        private CreateMode createMode;
        private final Set<Watcher> dataWatcherSet = new HashSet<Watcher>();
        private final Set<Watcher> childWatcherSet = new HashSet<Watcher>();

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

        public Set<Watcher> getDataWatcherSet() {
            return dataWatcherSet;
        }

        public Set<Watcher> getChildWatcherSet() {
            return childWatcherSet;
        }
    }

    private final MockZkNode rootNode = new MockZkNode(null, "", null, CreateMode.PERSISTENT);

    private final Set<Watcher> watcherSet = new HashSet<Watcher>(8, 0.9f);

    @Override
    public void process(WatchedEvent event) {
        for (Watcher watcher : watcherSet) {
            watcher.process(event);
        }
    }

    private void dataWatchNode(String path, Watcher watcher) {
        MockZkNode node = findNode(path);
        node.getDataWatcherSet().add(watcher);
    }

    private void childWatchNode(String path, Watcher watcher) {
        MockZkNode node = findNode(path);
        node.getChildWatcherSet().add(watcher);
    }

    void emitWatchedEvent(String path, EventType eventType, KeeperState keeperState) {
        logger.debug("Emitting event:  path={}; eventType={}; keeperState={}", new Object[] { path, eventType,
                keeperState });

        WatchedEvent event = new WatchedEvent(eventType, keeperState, path);
        MockZkNode node = findNode(path);
        if (node != null) {
            ArrayList<Watcher> watchersToRemove = new ArrayList<Watcher>();
            if (eventType == EventType.NodeDataChanged || eventType == EventType.NodeCreated
                    || eventType == EventType.NodeDeleted) {
                for (Watcher watcher : node.getDataWatcherSet()) {
                    watcher.process(event);
                    watchersToRemove.add(watcher);
                }

                // remove watches after single use, like real ZK behavior
                if (watchersToRemove.size() > 0) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Removing {} data watchers after emitting event:  path={}", watchersToRemove
                                .size(), path);
                    }
                    for (Watcher watcher : watchersToRemove) {
                        node.getDataWatcherSet().remove(watcher);
                    }
                }
            } else if (eventType == EventType.NodeChildrenChanged) {
                for (Watcher watcher : node.getChildWatcherSet()) {
                    watcher.process(event);
                    watchersToRemove.add(watcher);
                }

                // remove watches after single use, like real ZK behavior
                if (watchersToRemove.size() > 0) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Removing {} child watchers after emitting event:  path={}", watchersToRemove
                                .size(), path);
                    }
                    for (Watcher watcher : watchersToRemove) {
                        node.getChildWatcherSet().remove(watcher);
                    }
                }
            }

        }

        // alert all watchers for this event type
        if (eventType == EventType.None) {
            for (Watcher watcher : watcherSet) {
                watcher.process(event);
            }
        }
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
                logger.debug("Did not find node:  token='{}'", token);
                return null;
            } else {
                logger.debug("Found node:  token='{}'", token);
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
        watcherSet.add(watcher);
    }

    @Override
    public void close() {
    }

    @Override
    public Stat exists(String path, boolean watch) throws KeeperException, InterruptedException {
        if (watch) {
            dataWatchNode(path, this);
        }
        MockZkNode node = findNode(path);
        return node != null ? node.getStat() : null;
    }

    @Override
    public List<String> getChildren(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
        if (watch) {
            childWatchNode(path, this);
        }
        MockZkNode node = findNode(path);
        copyStat(node.getStat(), stat);
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
            childWatchNode(path, this);
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
            dataWatchNode(path, this);
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
        MockZkNode nodeToCreateIn = findNode(path, -1);
        if (nodeToCreateIn == null) {
            throw new KeeperException.NoNodeException();
        }

        nodeToCreateIn.putChild(new MockZkNode(nodeToCreateIn, pathLeaf(path), data, createMode));

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

    public static void main(String[] args) throws Exception {
        ZkClient zkClient = new ResilientZooKeeper("localhost:2181", 15000);
        zkClient.delete("/test", -1);
        zkClient.close();
        zkClient.exists("/test", false);

        // ZooKeeper zkClient = new ZooKeeper("localhost:2181", 15000, null);
        // zkClient.exists("/test", false);
        // zkClient.close();
        // zkClient.exists("/test", false);

    }
}
