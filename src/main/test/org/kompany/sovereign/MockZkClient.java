package org.kompany.sovereign;

import java.util.ArrayList;
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

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.kompany.sovereign.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mock ZkClient that mimics ZooKeeper functionality for unit tests.
 * 
 * @author ypai
 * 
 */
public class MockZkClient implements ZkClient {

    private static final Logger logger = LoggerFactory.getLogger(MockZkClient.class);

    private static final Pattern PATTERN_PATH_TOKENIZER = Pattern.compile("/");

    public static class ZkNode {
        private byte[] data;
        private Stat stat = new Stat();
        private final Map<String, ZkNode> children = new HashMap<String, ZkNode>(8);
        private final AtomicInteger sequence = new AtomicInteger(0);
        private final ZkNode parent;
        private final String name;
        private CreateMode createMode;

        public ZkNode(ZkNode parent, String name, CreateMode createMode) {
            this.parent = parent;
            this.name = name;
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

        public ZkNode getParent() {
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
        }

        public synchronized Stat getStat() {
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

        public synchronized void putChild(ZkNode node) {
            this.children.put(node.getName(), node);
        }

        public synchronized ZkNode getChild(String name) {
            return this.children.get(name);
        }

        public synchronized ZkNode removeChild(String name) {
            return this.children.remove(name);
        }

    }

    private static final Watcher DEFAULT_WATCHER = new Watcher() {
        @Override
        public void process(WatchedEvent arg0) {

        }
    };

    private final ZkNode rootNode = new ZkNode(null, "", CreateMode.PERSISTENT);

    private final Set<Watcher> watcherSet = new HashSet<Watcher>(8, 0.9f);

    private final ConcurrentMap<String, Set<Watcher>> watchedNodeMap = new ConcurrentHashMap<String, Set<Watcher>>();

    private void watchNode(String path, Watcher watcher) {
        Set<Watcher> watcherSet = this.watchedNodeMap.get(path);
        if (watcherSet == null) {
            Set<Watcher> newWatcherSet = Collections.newSetFromMap(new ConcurrentHashMap<Watcher, Boolean>());
            watcherSet = this.watchedNodeMap.putIfAbsent(path, newWatcherSet);
            if (watcherSet == null) {
                watcherSet = newWatcherSet;
            }
        }
        watcherSet.add(watcher);
    }

    private void emitWatchedEvent(String path, EventType eventType, KeeperState keeperState) {
        WatchedEvent event = new WatchedEvent(eventType, keeperState, path);
        for (Watcher watcher : watcherSet) {
            watcher.process(event);
        }
    }

    private ZkNode findNode(String path) {
        String[] tokens = PATTERN_PATH_TOKENIZER.split(path);
        ZkNode currentNode = rootNode;
        for (String token : tokens) {
            if (token != null && !token.isEmpty()) {
                currentNode = rootNode.getChild(token);
            }
            if (currentNode == null) {
                return null;
            }
        }
        return currentNode;
    }

    /**
     * @param path
     * @return true if we get it
     */
    private boolean deleteNode(String path) {
        String[] tokens = PATTERN_PATH_TOKENIZER.split(path);
        ZkNode currentNode = rootNode;
        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i];
            if (token != null && !token.isEmpty()) {
                if (i == tokens.length - 1) {
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
            watchNode(path, watcher);
        }
        ZkNode node = findNode(path);
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
            watchNode(path, DEFAULT_WATCHER);
        }
        ZkNode node = findNode(path);
        return node != null ? node.getStat() : null;
    }

    @Override
    public List<String> getChildren(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
        if (watch) {
            watchNode(path, DEFAULT_WATCHER);
        }
        ZkNode node = findNode(path);
        return node != null ? node.getChildList() : null;
    }

    @Override
    public List<String> getChildren(String path, Watcher watcher) throws KeeperException, InterruptedException {
        if (watcher != null) {
            watchNode(path, watcher);
        }
        ZkNode node = findNode(path);
        return node != null ? node.getChildList() : null;
    }

    @Override
    public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException {
        if (watch) {
            watchNode(path, DEFAULT_WATCHER);
        }
        ZkNode node = findNode(path);
        return node != null ? node.getChildList() : null;
    }

    @Override
    public Stat setData(String path, byte[] data, int version) throws KeeperException, InterruptedException {
        ZkNode node = findNode(path);
        if (node != null) {
            node.setData(data);
            return node.getStat();
        }
        return null;
    }

    @Override
    public byte[] getData(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
        if (watch) {
            watchNode(path, DEFAULT_WATCHER);
        }

        ZkNode node = findNode(path);
        if (node != null) {
            return node.getData();
        }
        return null;
    }

    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException,
            InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void delete(String path, int version) throws InterruptedException, KeeperException {
        deleteNode(path);
    }

}
