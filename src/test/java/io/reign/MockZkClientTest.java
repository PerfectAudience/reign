package io.reign;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import io.reign.MockZkClient.MockZkNode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author ypai
 * 
 */
public class MockZkClientTest {

    public static final List<ACL> DEFAULT_ACL_LIST = new ArrayList<ACL>();
    static {
        DEFAULT_ACL_LIST.add(new ACL(ZooDefs.Perms.ALL, new Id("world", "anyone")));
    }

    private MockZkClient zkClient;

    @Before
    public void setUp() throws Exception {
        zkClient = new MockZkClient();
    }

    @Test
    public void testExistsStringWatcher() throws Exception {
        // register watcher
        final AtomicBoolean eventReceived = new AtomicBoolean(false);
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent arg0) {
                eventReceived.set(true);
            }
        };
        zkClient.register(watcher);

        zkClient.create("/testExistsStringWatcher", null, DEFAULT_ACL_LIST, CreateMode.PERSISTENT);
        assertTrue(zkClient.exists("/testExistsStringWatcher", watcher) != null);

        zkClient.delete("/testExistsStringWatcher", -1);
        assertTrue(zkClient.exists("/testExistsStringWatcher", null) == null);
        assertTrue(eventReceived.get());

        // delete node that no longer exists, should NOT receive event
        eventReceived.set(false);
        try {
            zkClient.delete("/testExistsStringWatcher", -1);

            // should not get here
            assertTrue(false);
        } catch (KeeperException e) {
            assertTrue(e.code() == KeeperException.Code.NONODE);
        }
        assertFalse(eventReceived.get());

        // create node again and delete without watch
        eventReceived.set(false);
        zkClient.create("/testExistsStringWatcher", null, DEFAULT_ACL_LIST, CreateMode.PERSISTENT);
        assertTrue(zkClient.exists("/testExistsStringWatcher", null) != null);
        zkClient.delete("/testExistsStringWatcher", -1);
        assertFalse(eventReceived.get());

    }

    @Test
    public void testRegister() {
        final AtomicBoolean eventReceived = new AtomicBoolean(false);
        Watcher watcher = new Watcher() {

            @Override
            public void process(WatchedEvent arg0) {
                eventReceived.set(true);
            }

        };
        zkClient.register(watcher);
        zkClient.emitWatchedEvent(null, EventType.None, Event.KeeperState.SyncConnected);

        assertTrue("Did not receive event!", eventReceived.get());
    }

    @Test
    public void testClose() throws Exception {
        ZkClient zkClient = new MockZkClient();
        zkClient.exists("/test", false);
        zkClient.close();
        try {
            zkClient.exists("/test", false);
        } catch (KeeperException e) {
            assertTrue(e.code() == KeeperException.Code.SESSIONEXPIRED);
        }
    }

    @Test
    public void testExistsStringBoolean() throws Exception {
        zkClient.create("/testExistsStringBoolean", null, DEFAULT_ACL_LIST, CreateMode.PERSISTENT);
        assertTrue(zkClient.exists("/testExistsStringBoolean", true) != null);

        // register watcher
        final AtomicBoolean eventReceived = new AtomicBoolean(false);
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent arg0) {
                eventReceived.set(true);
            }
        };
        zkClient.register(watcher);

        zkClient.delete("/testExistsStringBoolean", -1);
        assertTrue(zkClient.exists("/testExistsStringBoolean", false) == null);
        assertTrue(eventReceived.get());

        // delete node that no longer exists, should NOT receive event
        eventReceived.set(false);
        try {
            zkClient.delete("/testExistsStringBoolean", -1);

            // should not get here
            assertTrue(false);
        } catch (KeeperException e) {
            assertTrue(e.code() == KeeperException.Code.NONODE);
        }
        assertFalse(eventReceived.get());

        // create node again and delete without watch
        eventReceived.set(false);
        zkClient.create("/testExistsStringBoolean", null, DEFAULT_ACL_LIST, CreateMode.PERSISTENT);
        assertTrue(zkClient.exists("/testExistsStringBoolean", false) != null);
        zkClient.delete("/testExistsStringBoolean", -1);
        assertFalse(eventReceived.get());

    }

    @Test
    public void testGetChildrenStringBooleanStat() throws Exception {
        // register watcher
        final AtomicBoolean eventReceived = new AtomicBoolean(false);
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent arg0) {
                eventReceived.set(true);
            }
        };
        zkClient.register(watcher);

        Stat stat = new Stat();

        zkClient.create("/testGetChildrenStringBooleanStat", null, DEFAULT_ACL_LIST, CreateMode.PERSISTENT);
        zkClient.create("/testGetChildrenStringBooleanStat/child1", null, DEFAULT_ACL_LIST, CreateMode.PERSISTENT);
        zkClient.create("/testGetChildrenStringBooleanStat/child2", null, DEFAULT_ACL_LIST, CreateMode.PERSISTENT);
        assertTrue(zkClient.exists("/testGetChildrenStringBooleanStat", watcher) != null);
        List<String> children = zkClient.getChildren("/testGetChildrenStringBooleanStat", true, stat);
        assertTrue(stat.getNumChildren() == 2);
        assertTrue(children.size() == 2 && children.contains("child1") && children.contains("child2"));

        // check delete
        zkClient.delete("/testGetChildrenStringBooleanStat/child2", -1);
        children = zkClient.getChildren("/testGetChildrenStringBooleanStat", false, stat);
        assertTrue(stat.getNumChildren() == 1);
        assertTrue(children.size() == 1 && children.contains("child1") && !children.contains("child2"));
        assertTrue(zkClient.exists("/testGetChildrenStringBooleanStat/child2", null) == null);
        assertTrue(eventReceived.get());

        // delete node that no longer exists, should NOT receive event
        eventReceived.set(false);
        try {
            zkClient.delete("/testGetChildrenStringBooleanStat/child2", -1);
            assertTrue(false);
        } catch (KeeperException e) {
            assertTrue(e.code() == KeeperException.Code.NONODE);

        }
        assertFalse(eventReceived.get());

        // create node again and delete without watch
        eventReceived.set(false);
        zkClient.create("/testGetChildrenStringBooleanStat/child2", null, DEFAULT_ACL_LIST, CreateMode.PERSISTENT);
        assertTrue(zkClient.exists("/testGetChildrenStringBooleanStat/child2", null) != null);
        zkClient.delete("/testGetChildrenStringBooleanStat/child2", -1);
        assertFalse(eventReceived.get());
    }

    @Test
    public void testGetChildrenStringWatcher() throws Exception {
        // register watcher
        final AtomicBoolean eventReceived = new AtomicBoolean(false);
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent arg0) {
                eventReceived.set(true);
            }
        };
        zkClient.register(watcher);

        zkClient.create("/testGetChildrenStringWatcher", null, DEFAULT_ACL_LIST, CreateMode.PERSISTENT);
        zkClient.create("/testGetChildrenStringWatcher/child1", null, DEFAULT_ACL_LIST, CreateMode.PERSISTENT);
        zkClient.create("/testGetChildrenStringWatcher/child2", null, DEFAULT_ACL_LIST, CreateMode.PERSISTENT);
        assertTrue(zkClient.exists("/testGetChildrenStringWatcher", watcher) != null);
        List<String> children = zkClient.getChildren("/testGetChildrenStringWatcher", watcher);
        assertTrue(children.size() == 2 && children.contains("child1") && children.contains("child2"));

        // check delete
        zkClient.delete("/testGetChildrenStringWatcher/child2", -1);
        children = zkClient.getChildren("/testGetChildrenStringWatcher", null);
        assertTrue(children.size() == 1 && children.contains("child1") && !children.contains("child2"));
        assertTrue(zkClient.exists("/testGetChildrenStringWatcher/child2", null) == null);
        assertTrue(eventReceived.get());

        // delete node that no longer exists, should NOT receive event
        eventReceived.set(false);
        try {
            zkClient.delete("/testGetChildrenStringWatcher/child2", -1);
            assertTrue(false);
        } catch (KeeperException e) {
            assertTrue(e.code() == KeeperException.Code.NONODE);

        }
        assertFalse(eventReceived.get());

        // create node again and delete without watch
        eventReceived.set(false);
        zkClient.create("/testGetChildrenStringWatcher/child2", null, DEFAULT_ACL_LIST, CreateMode.PERSISTENT);
        assertTrue(zkClient.exists("/testGetChildrenStringWatcher/child2", null) != null);
        zkClient.delete("/testGetChildrenStringWatcher/child2", -1);
        assertFalse(eventReceived.get());
    }

    @Test
    public void testGetChildrenStringBoolean() throws Exception {
        // register watcher
        final AtomicBoolean eventReceived = new AtomicBoolean(false);
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent arg0) {
                eventReceived.set(true);
            }
        };
        zkClient.register(watcher);

        zkClient.create("/testGetChildrenStringWatcher", null, DEFAULT_ACL_LIST, CreateMode.PERSISTENT);
        zkClient.create("/testGetChildrenStringWatcher/child1", null, DEFAULT_ACL_LIST, CreateMode.PERSISTENT);
        zkClient.create("/testGetChildrenStringWatcher/child2", null, DEFAULT_ACL_LIST, CreateMode.PERSISTENT);
        assertTrue(zkClient.exists("/testGetChildrenStringWatcher", watcher) != null);
        List<String> children = zkClient.getChildren("/testGetChildrenStringWatcher", true);
        assertTrue(children.size() == 2 && children.contains("child1") && children.contains("child2"));

        // check delete
        zkClient.delete("/testGetChildrenStringWatcher/child2", -1);
        children = zkClient.getChildren("/testGetChildrenStringWatcher", false);
        assertTrue(children.size() == 1 && children.contains("child1") && !children.contains("child2"));
        assertTrue(zkClient.exists("/testGetChildrenStringWatcher/child2", null) == null);
        assertTrue(eventReceived.get());

        // delete node that no longer exists, should NOT receive event
        eventReceived.set(false);
        try {
            zkClient.delete("/testGetChildrenStringWatcher/child2", -1);
            assertTrue(false);
        } catch (KeeperException e) {
            assertTrue(e.code() == KeeperException.Code.NONODE);

        }
        assertFalse(eventReceived.get());

        // create node again and delete without watch
        eventReceived.set(false);
        zkClient.create("/testGetChildrenStringWatcher/child2", null, DEFAULT_ACL_LIST, CreateMode.PERSISTENT);
        assertTrue(zkClient.exists("/testGetChildrenStringWatcher/child2", null) != null);
        zkClient.delete("/testGetChildrenStringWatcher/child2", -1);
        assertFalse(eventReceived.get());
    }

    @Test
    public void testSetData() throws Exception {
        MockZkNode node;
        byte[] bytes = new byte[1];

        // try to set data on a node that doesn't exist
        try {
            zkClient.setData("/testSetData", bytes, -1);
            assertTrue(false);
        } catch (KeeperException e) {
            assertTrue(e.code() == KeeperException.Code.NONODE);

        }

        // create in path
        zkClient.create("/testSetData", null, DEFAULT_ACL_LIST, CreateMode.PERSISTENT);
        node = zkClient.findNode("/testSetData");
        assertTrue("testSetData".equals(node.getName()));
        assertTrue(node.getData() == null);

        // set data
        zkClient.setData("/testSetData", bytes, -1);
        node = zkClient.findNode("/testSetData");
        assertTrue("testSetData".equals(node.getName()));
        assertTrue(node.getData() == bytes);

    }

    @Test
    public void testGetData() throws Exception {
        MockZkNode node;
        byte[] bytes = new byte[1];
        byte[] bytesNew = new byte[] { (byte) 1 };

        // create in path
        zkClient.create("/testGetData", null, DEFAULT_ACL_LIST, CreateMode.PERSISTENT);
        node = zkClient.findNode("/testGetData");
        assertTrue("testGetData".equals(node.getName()));
        assertTrue(node.getData() == null);

        // set data
        Thread.sleep(10);
        zkClient.setData("/testGetData", bytes, -1);
        node = zkClient.findNode("/testGetData");
        assertTrue("testGetData".equals(node.getName()));
        assertTrue(node.getData() == bytes);

        // get data
        Stat stat = new Stat();
        byte[] data = zkClient.getData("/testGetData", true, stat);
        assertTrue(data == bytes);
        assertTrue(System.currentTimeMillis() - stat.getMtime() < 10000);
        assertTrue(stat.getMtime() > stat.getCtime());

        /** test watchers **/
        // register watcher
        final AtomicBoolean eventReceived = new AtomicBoolean(false);
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent arg0) {
                eventReceived.set(true);
            }
        };
        zkClient.register(watcher);

        // set data with something different
        zkClient.setData("/testGetData", bytesNew, -1);

        assertTrue(eventReceived.get());

    }

    @Test
    public void testCreate() throws Exception {
        MockZkNode node;
        byte[] bytes = new byte[1];

        // create in path
        zkClient.create("/testCreate", null, DEFAULT_ACL_LIST, CreateMode.PERSISTENT);
        node = zkClient.findNode("/testCreate");
        assertTrue("testCreate".equals(node.getName()));
        assertTrue(node.getData() == null);

        // create another subnode
        zkClient.create("/testCreate/anotherNode", bytes, DEFAULT_ACL_LIST, CreateMode.PERSISTENT);
        node = zkClient.findNode("/testCreate/anotherNode");
        assertTrue("anotherNode".equals(node.getName()));
        assertTrue(node.getData() == bytes);

        // try to create in path that does not already exist
        try {
            zkClient.create("/testCreate/thisShouldFail/yes", bytes, DEFAULT_ACL_LIST, CreateMode.PERSISTENT);
            assertTrue(false);
        } catch (KeeperException e) {
            assertTrue(e.code() == KeeperException.Code.NONODE);

        }

    }

    @Test
    public void testDelete() throws Exception {
        MockZkNode node;
        byte[] bytes = new byte[1];

        // create in path
        zkClient.create("/testDelete", null, DEFAULT_ACL_LIST, CreateMode.PERSISTENT);
        node = zkClient.findNode("/testDelete");
        assertTrue("testDelete".equals(node.getName()));
        assertTrue(node.getData() == null);

        // create another subnode
        zkClient.create("/testDelete/anotherNode", bytes, DEFAULT_ACL_LIST, CreateMode.PERSISTENT);
        node = zkClient.findNode("/testDelete/anotherNode");
        assertTrue("anotherNode".equals(node.getName()));
        assertTrue(node.getData() == bytes);

        // should not be able to delete parent with children
        try {
            zkClient.delete("/testDelete", -1);
            assertTrue(false);
        } catch (KeeperException e) {
            assertTrue(e.code() == KeeperException.Code.NOTEMPTY);

        }

        // should get exception when trying to delete node that doesn't exist
        try {
            zkClient.delete("/doesNotExist", -1);
            assertTrue(false);
        } catch (KeeperException e) {
            assertTrue(e.code() == KeeperException.Code.NONODE);

        }

        // delete
        zkClient.delete("/testDelete/anotherNode", -1);
        node = zkClient.findNode("/testDelete/anotherNode");
        assertTrue(node == null);

    }

}
