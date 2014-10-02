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
                presenceService.announce("clusterTestWaitUntilAvailableService", "serviceA1.1", true);
            }
        };
        t1.start();

        // wait until service is available
        long start = System.currentTimeMillis();
        ServiceInfo serviceInfo = presenceService.waitUntilAvailable("clusterTestWaitUntilAvailableService",
                "serviceA1.1", -1);
        long end = System.currentTimeMillis();

        assertTrue(
                "serviceInfo==null = " + (serviceInfo == null) + "; nodeList="
                        + (serviceInfo != null ? serviceInfo.getNodeIdList() : null), serviceInfo != null
                        && serviceInfo.getNodeIdList().size() > 0);
        assertTrue("Not expected time:  " + (end - start), end - start >= 3000);

        // restore to previous state at beginning of test
        presenceService.hide("clusterTestWaitUntilAvailableService", "serviceA1.1");
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
                presenceService.announce("clusterTestWaitUntilAvailableNode", "serviceA1.2", true);
            }
        };
        t1.start();

        // wait until service is available
        long start = System.currentTimeMillis();
        NodeInfo nodeInfo = presenceService.waitUntilAvailable("clusterTestWaitUntilAvailableNode", "serviceA1.2",
                nodeId, -1);
        long end = System.currentTimeMillis();

        assertTrue("nodeInfo==null = " + (nodeInfo == null), nodeInfo != null);
        assertTrue("Not expected time:  " + (end - start), end - start >= 3000);

        // restore to previous state at beginning of test
        presenceService.hide("clusterTestWaitUntilAvailableNode", "serviceA1.2");
    }

    @Test
    public void testGetClusters() {

        presenceService.announce("clusterTestGetClusters", "serviceA1", true);

        // announcements are processed asynchronously, so wait until services are available
        presenceService.waitUntilAvailable("clusterTestGetClusters", "serviceA1", -1);

        List<String> clusterIdList = presenceService.getClusters();

        // should be 3 clusters, including the default "reign" cluster
        assertTrue("Should be at least 2 but got " + clusterIdList.size() + ":  " + clusterIdList,
                clusterIdList.size() >= 2);
        assertTrue(clusterIdList.contains("clusterTestGetClusters"));

        // restore to previous state at beginning of test
        presenceService.hide("clusterTestGetClusters", "serviceA1");
    }

    @Test
    public void testServiceObserver() throws Exception {
        final AtomicReference<ServiceInfo> serviceInfoRef = new AtomicReference<ServiceInfo>();
        ServiceInfo serviceInfo = presenceService.getServiceInfo("clusterTestServiceObserver", "serviceC1",
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

        presenceService.announce("clusterTestServiceObserver", "serviceC1", true);
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
        ServiceNodeInfo nodeInfo = presenceService.getNodeInfo("clusterTestNodeObserver", "serviceD1", nodeId,
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
        presenceService.announce("clusterTestNodeObserver", "serviceD1", true,
                Structs.<String, String> map().kv("foo", "bar"));
        synchronized (nodeInfoRef) {
            nodeInfoRef.wait(5000);
        }

        assertTrue(nodeInfoRef.get().getAttribute("foo").equals("bar"));

        // change something to invoke observer
        presenceService.announce("clusterTestNodeObserver", "serviceD1", true,
                Structs.<String, String> map().kv("foo", "bar").kv("lady", "liberty"));
        synchronized (nodeInfoRef) {
            nodeInfoRef.wait(5000);
        }

        assertTrue(nodeInfoRef.get().getAttribute("foo").equals("bar")
                && nodeInfoRef.get().getAttribute("lady").equals("liberty"));
    }

    @Test
    public void testGetServices() throws Exception {
        presenceService.announce("clusterTestGetServices", "serviceA1", true);
        presenceService.announce("clusterTestGetServices", "serviceA2", true);

        presenceService.waitUntilAvailable("clusterTestGetServices", "serviceA1", -1);
        presenceService.waitUntilAvailable("clusterTestGetServices", "serviceA2", -1);

        List<String> serviceIdList = presenceService.getServices("clusterTestGetServices");
        assertTrue("Should be 2 but got " + serviceIdList.size() + ":  " + serviceIdList, serviceIdList.size() == 2);
        assertTrue(serviceIdList.contains("serviceA1"));
        assertTrue(serviceIdList.contains("serviceA2"));
    }

    @Test
    public void testGetNodeInfo() throws Exception {
        presenceService.announce("clusterTestGetNodeInfo", "serviceA1", true);

        ServiceInfo serviceInfo = presenceService.waitUntilAvailable("clusterTestGetNodeInfo", "serviceA1", -1);
        assertTrue("service nodes = " + serviceInfo.getNodeIdList(), serviceInfo.getNodeIdList().size() > 0);

        ServiceNodeInfo nodeInfo = presenceService.getNodeInfo("clusterTestGetNodeInfo", "serviceA1", nodeId);

        assertTrue("nodeInfo should not be null", nodeInfo != null);
        assertTrue("clusterTestGetNodeInfo".equals(nodeInfo.getClusterId()));
        assertTrue("serviceA1".equals(nodeInfo.getServiceId()));
        assertTrue(nodeId.equals(nodeInfo.getNodeId().toString()));
    }

    @Test
    public void testHide() throws Exception {
        Reign reign = MasterTestSuite.getReign();
        PathScheme pathScheme = reign.getPathScheme();
        String nodePath = pathScheme.joinTokens("clusterTestHide", "serviceA1", nodeId);
        String path = pathScheme.getAbsolutePath(PathType.PRESENCE, nodePath);

        presenceService.announce("clusterTestHide", "serviceA1", true);

        // allow up to 10 seconds for to take effect
        for (int i = 0; i < 10; i++) {
            if (reign.getZkClient().exists(path, false) != null) {
                break;
            }
            Thread.sleep(1000);
        }

        assertTrue(reign.getZkClient().exists(path, false) != null);

        presenceService.hide("clusterTestHide", "serviceA1");

        // allow up to 10 seconds for to take effect
        for (int i = 0; i < 10; i++) {
            if (reign.getZkClient().exists(path, false) == null) {
                break;
            }
            Thread.sleep(1000);
        }

        assertTrue(reign.getZkClient().exists(path, false) == null);

    }

    @Test
    public void testShow() throws Exception {
        Reign reign = MasterTestSuite.getReign();
        PathScheme pathScheme = reign.getPathScheme();
        String nodePath = pathScheme.joinTokens("clusterTestShow", "serviceA1", nodeId);
        String path = pathScheme.getAbsolutePath(PathType.PRESENCE, nodePath);

        presenceService.announce("clusterTestShow", "serviceA1", false);

        // allow up to 10 seconds for to take effect
        for (int i = 0; i < 10; i++) {
            if (reign.getZkClient().exists(path, false) == null) {
                break;
            }
            Thread.sleep(1000);
        }

        assertTrue(reign.getZkClient().exists(path, false) == null);

        presenceService.show("clusterTestShow", "serviceA1");

        // allow up to 10 seconds for to take effect
        for (int i = 0; i < 10; i++) {
            if (reign.getZkClient().exists(path, false) != null) {
                break;
            }
            Thread.sleep(1000);
        }

        assertTrue(reign.getZkClient().exists(path, false) != null);
    }
}
