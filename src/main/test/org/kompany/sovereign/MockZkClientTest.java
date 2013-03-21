package org.kompany.sovereign;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.junit.Before;
import org.junit.Test;

public class MockZkClientTest {

    private MockZkClient zkClient;

    @Before
    public void setUp() throws Exception {
        zkClient = new MockZkClient();
    }

    @Test
    public void testExistsStringWatcher() {
        fail("Not yet implemented");
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
    public void testClose() {
        fail("Not yet implemented");
    }

    @Test
    public void testExistsStringBoolean() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetChildrenStringBooleanStat() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetChildrenStringWatcher() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetChildrenStringBoolean() {
        fail("Not yet implemented");
    }

    @Test
    public void testSetData() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetData() {
        fail("Not yet implemented");
    }

    @Test
    public void testCreate() {
        fail("Not yet implemented");
    }

    @Test
    public void testDelete() {
        fail("Not yet implemented");
    }

}
