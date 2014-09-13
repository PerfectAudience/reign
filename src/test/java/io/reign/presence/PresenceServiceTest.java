package io.reign.presence;

import static org.junit.Assert.assertTrue;
import io.reign.MasterTestSuite;
import io.reign.NodeInfo;
import io.reign.PathScheme;
import io.reign.PathType;
import io.reign.Reign;
import io.reign.ServiceNodeInfo;
import io.reign.util.Structs;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PresenceServiceTest {

    private static final Logger logger = LoggerFactory.getLogger(PresenceServiceTest.class);

    private PresenceService presenceService;
    private String nodeId;

    @Before
    public void setUp() throws Exception {
        presenceService = MasterTestSuite.getReign().getService("presence");
        nodeId = MasterTestSuite.getReign().getNodeIdProvider().get();
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testWaitUntilAvailableService() throws Exception {
        Thread t1 = new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                }
                presenceService.announce("clusterA", "serviceA1.1", true);
            }
        };
        t1.start();

        // wait until service is available
        long start = System.currentTimeMillis();
        ServiceInfo serviceInfo = presenceService.waitUntilAvailable("clusterA", "serviceA1.1", -1);
        long end = System.currentTimeMillis();

        assertTrue(
                "serviceInfo==null = " + (serviceInfo == null) + "; nodeList="
                        + (serviceInfo != null ? serviceInfo.getNodeIdList() : null), serviceInfo != null
                        && serviceInfo.getNodeIdList().size() > 0);
        assertTrue("Not expected time:  " + (end - start), end - start >= 3000);

        // restore to previous state at beginning of test
        presenceService.hide("clusterA", "serviceA1.1");
    }

    @Test
    public void testWaitUntilAvailableNode() throws Exception {
        Thread t1 = new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                }
                presenceService.announce("clusterA", "serviceA1.2", true);
            }
        };
        t1.start();

        // wait until service is available
        long start = System.currentTimeMillis();
        NodeInfo nodeInfo = presenceService.waitUntilAvailable("clusterA", "serviceA1.2", nodeId, -1);
        long end = System.currentTimeMillis();

        assertTrue("nodeInfo==null = " + (nodeInfo == null), nodeInfo != null);
        assertTrue("Not expected time:  " + (end - start), end - start >= 3000);

        // restore to previous state at beginning of test
        presenceService.hide("clusterA", "serviceA1.2");
    }

    @Test
    public void testLookupClusters() {

        presenceService.announce("clusterA", "serviceA1", true);
        presenceService.announce("clusterB", "serviceB1", true);

        // announcements are processed asynchronously, so wait until services are available
        presenceService.waitUntilAvailable("clusterA", "serviceA1", -1);
        presenceService.waitUntilAvailable("clusterB", "serviceB1", -1);

        List<String> clusterIdList = presenceService.getClusters();

        // should be 3 clusters, including the default "reign" cluster
        assertTrue("Should be 3 but got " + clusterIdList.size() + ":  " + clusterIdList, clusterIdList.size() == 3);
        assertTrue(clusterIdList.contains("clusterA"));
        assertTrue(clusterIdList.contains("clusterB"));

        // restore to previous state at beginning of test
        presenceService.hide("clusterA", "serviceA1");
        presenceService.hide("clusterB", "serviceB1");
    }

    @Test
    public void testServiceObserver() throws Exception {
        final AtomicReference<ServiceInfo> serviceInfoRef = new AtomicReference<ServiceInfo>();
        ServiceInfo serviceInfo = presenceService.getServiceInfo("clusterC", "serviceC1",
                new PresenceObserver<ServiceInfo>() {
                    @Override
                    public void updated(ServiceInfo updated, ServiceInfo previous) {
                        logger.debug("UPDATED:  {}",
                                ReflectionToStringBuilder.toString(updated, ToStringStyle.DEFAULT_STYLE));
                        serviceInfoRef.set(updated);
                        synchronized (serviceInfoRef) {
                            logger.debug("NOTIFY all...");
                            serviceInfoRef.notifyAll();
                        }
                    }
                });

        assertTrue(serviceInfo != null);
        assertTrue("serviceInfo=" + ReflectionToStringBuilder.toString(serviceInfo, ToStringStyle.DEFAULT_STYLE),
                serviceInfo.getNodeIdList().size() == 0);

        presenceService.announce("clusterC", "serviceC1", true);
        synchronized (serviceInfoRef) {
            logger.debug("WAITING to be notified...");
            serviceInfoRef.wait(5000);
        }

        assertTrue(serviceInfoRef.get() != null);
        assertTrue("Expected 1, got " + serviceInfoRef.get().getNodeIdList().size(), serviceInfoRef.get()
                .getNodeIdList().size() == 1);

    }

    @Test
    public void testNodeObserver() throws Exception {
        final AtomicReference<ServiceNodeInfo> nodeInfoRef = new AtomicReference<ServiceNodeInfo>();
        ServiceNodeInfo nodeInfo = presenceService.getNodeInfo("clusterD", "serviceD1", nodeId,
                new PresenceObserver<ServiceNodeInfo>() {
                    @Override
                    public void updated(ServiceNodeInfo updated, ServiceNodeInfo previous) {
                        nodeInfoRef.set(updated);
                        synchronized (nodeInfoRef) {
                            nodeInfoRef.notifyAll();
                        }
                    }
                });

        assertTrue("nodeInfo=" + ReflectionToStringBuilder.toString(nodeInfo, ToStringStyle.DEFAULT_STYLE),
                nodeInfo == null);

        // change something to invoke observer
        presenceService.announce("clusterD", "serviceD1", true, Structs.<String, String> map().kv("foo", "bar"));
        synchronized (nodeInfoRef) {
            nodeInfoRef.wait(5000);
        }

        assertTrue(nodeInfoRef.get().getAttribute("foo").equals("bar"));

        // change something to invoke observer
        presenceService.announce("clusterD", "serviceD1", true,
                Structs.<String, String> map().kv("foo", "bar").kv("lady", "liberty"));
        synchronized (nodeInfoRef) {
            nodeInfoRef.wait(5000);
        }

        assertTrue(nodeInfoRef.get().getAttribute("foo").equals("bar")
                && nodeInfoRef.get().getAttribute("lady").equals("liberty"));
    }

    @Test
    public void testLookupServices() throws Exception {
        presenceService.announce("clusterA", "serviceA1", true);
        presenceService.announce("clusterA", "serviceA2", true);

        presenceService.waitUntilAvailable("clusterA", "serviceA1", -1);
        presenceService.waitUntilAvailable("clusterA", "serviceA2", -1);

        List<String> serviceIdList = presenceService.getServices("clusterA");
        assertTrue("Should be 2 but got " + serviceIdList.size() + ":  " + serviceIdList, serviceIdList.size() == 2);
        assertTrue(serviceIdList.contains("serviceA1"));
        assertTrue(serviceIdList.contains("serviceA2"));
    }

    @Test
    public void testLookupNodeInfoStringStringString() throws Exception {
        presenceService.announce("clusterA", "serviceA1", true);

        ServiceInfo serviceInfo = presenceService.waitUntilAvailable("clusterA", "serviceA1", -1);
        assertTrue("service nodes = " + serviceInfo.getNodeIdList(), serviceInfo.getNodeIdList().size() > 0);

        ServiceNodeInfo nodeInfo = presenceService.getNodeInfo("clusterA", "serviceA1", nodeId);

        assertTrue("nodeInfo should not be null", nodeInfo != null);
        assertTrue("clusterA".equals(nodeInfo.getClusterId()));
        assertTrue("serviceA1".equals(nodeInfo.getServiceId()));
        assertTrue(nodeId.equals(nodeInfo.getNodeId().toString()));
    }

    @Test
    public void testHideStringString() throws Exception {
        presenceService.announce("clusterA", "serviceA1", true);
        presenceService.hide("clusterA", "serviceA1");

        Thread.sleep(1000);

        Reign reign = MasterTestSuite.getReign();
        PathScheme pathScheme = reign.getPathScheme();
        String nodePath = pathScheme.joinTokens("clusterA", "serviceA1", nodeId);
        String path = pathScheme.getAbsolutePath(PathType.PRESENCE, nodePath);
        assertTrue(reign.getZkClient().exists(path, false) == null);

        presenceService.show("clusterA", "serviceA1");

        Thread.sleep(1000);

        assertTrue(reign.getZkClient().exists(path, false) != null);
    }

    @Test
    public void testShowStringString() throws Exception {
        presenceService.announce("clusterA", "serviceA1", true);
        presenceService.hide("clusterA", "serviceA1");

        Thread.sleep(1000);

        Reign reign = MasterTestSuite.getReign();
        PathScheme pathScheme = reign.getPathScheme();
        String nodePath = pathScheme.joinTokens("clusterA", "serviceA1", nodeId);
        String path = pathScheme.getAbsolutePath(PathType.PRESENCE, nodePath);
        assertTrue(reign.getZkClient().exists(path, false) == null);

        presenceService.show("clusterA", "serviceA1");

        Thread.sleep(1000);

        assertTrue(reign.getZkClient().exists(path, false) != null);
    }
}
