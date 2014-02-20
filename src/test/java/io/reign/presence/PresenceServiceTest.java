package io.reign.presence;

import static org.junit.Assert.assertTrue;
import io.reign.MasterTestSuite;
import io.reign.PathScheme;
import io.reign.PathType;
import io.reign.Reign;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PresenceServiceTest {

    private static final Logger logger = LoggerFactory.getLogger(PresenceServiceTest.class);

    private PresenceService presenceService;

    @Before
    public void setUp() throws Exception {

        presenceService = MasterTestSuite.getReign().getService("presence");

        // // add some services
        // presenceService.announce("clusterA", "serviceA", false);
        // presenceService.announce("clusterA", "serviceB", true);
        // presenceService.announce("clusterB", "serviceC", true);
        // presenceService.announce("clusterB", "serviceD", false);

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
                presenceService.announce("clusterA", "serviceA1", true);
            }
        };
        t1.start();

        // wait until service is available
        long start = System.currentTimeMillis();
        ServiceInfo serviceInfo = presenceService.waitUntilAvailable("clusterA", "serviceA1", -1);
        long end = System.currentTimeMillis();

        assertTrue(
                "serviceInfo==null = " + (serviceInfo == null) + "; nodeList="
                        + (serviceInfo != null ? serviceInfo.getNodeIdList() : null), serviceInfo != null
                        && serviceInfo.getNodeIdList().size() > 0);
        assertTrue(end - start > 2500);

        // restore to previous state at beginning of test
        presenceService.hide("clusterA", "serviceA1");
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
                presenceService.announce("clusterA", "serviceA1", true);
            }
        };
        t1.start();

        // wait until service is available
        long start = System.currentTimeMillis();
        NodeInfo nodeInfo = presenceService.waitUntilAvailable("clusterA", "serviceA1", MasterTestSuite.getReign()
                .getCanonicalIdPathToken(), -1);
        long end = System.currentTimeMillis();

        assertTrue("nodeInfo==null = " + (nodeInfo == null), nodeInfo != null);
        assertTrue(end - start > 2500);

        // restore to previous state at beginning of test
        presenceService.hide("clusterA", "serviceA1");
    }

    @Test
    public void testLookupClusters() {

        presenceService.announce("clusterA", "serviceA1", true);
        presenceService.announce("clusterB", "serviceB1", true);

        // announcements are processed asynchronously, so wait until services are available
        presenceService.waitUntilAvailable("clusterA", "serviceA1", -1);
        presenceService.waitUntilAvailable("clusterB", "serviceB1", -1);

        List<String> clusterIdList = presenceService.lookupClusters();

        // should be 3 clusters, including the default "reign" cluster
        assertTrue("Should be 3 but got " + clusterIdList.size() + ":  " + clusterIdList, clusterIdList.size() == 3);
        assertTrue(clusterIdList.contains("clusterA"));
        assertTrue(clusterIdList.contains("clusterB"));

        // restore to previous state at beginning of test
        presenceService.hide("clusterA", "serviceA1");
        presenceService.hide("clusterB", "serviceB1");
    }

    @Test
    public void testLookupServices() throws Exception {
        presenceService.announce("clusterA", "serviceA1", true);
        presenceService.announce("clusterA", "serviceA2", true);

        presenceService.waitUntilAvailable("clusterA", "serviceA1", -1);
        presenceService.waitUntilAvailable("clusterA", "serviceA2", -1);

        List<String> serviceIdList = presenceService.lookupServices("clusterA");
        assertTrue("Should be 2 but got " + serviceIdList.size() + ":  " + serviceIdList, serviceIdList.size() == 2);
        assertTrue(serviceIdList.contains("serviceA1"));
        assertTrue(serviceIdList.contains("serviceA2"));
    }

    @Test
    public void testLookupNodeInfoStringStringString() throws Exception {
        presenceService.announce("clusterA", "serviceA1", true);

        Reign reign = MasterTestSuite.getReign();

        ServiceInfo serviceInfo = presenceService.waitUntilAvailable("clusterA", "serviceA1", -1);
        assertTrue("service nodes = " + serviceInfo.getNodeIdList(), serviceInfo.getNodeIdList().size() > 0);

        NodeInfo nodeInfo = presenceService.lookupNodeInfo("clusterA", "serviceA1", reign.getContext()
                .getCanonicalIdPathToken());

        assertTrue("nodeInfo should not be null", nodeInfo != null);
        assertTrue("clusterA".equals(nodeInfo.getClusterId()));
        assertTrue("serviceA1".equals(nodeInfo.getServiceId()));
        assertTrue(reign.getPathScheme().toPathToken(reign.getCanonicalId()).equals(nodeInfo.getNodeId()));
    }

    @Test
    public void testHideStringString() throws Exception {
        presenceService.announce("clusterA", "serviceA1", true);
        presenceService.hide("clusterA", "serviceA1");

        Thread.sleep(1000);

        Reign reign = MasterTestSuite.getReign();
        PathScheme pathScheme = reign.getPathScheme();
        String nodePath = pathScheme
                .joinTokens("clusterA", "serviceA1", pathScheme.toPathToken(reign.getCanonicalId()));
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
        String nodePath = pathScheme
                .joinTokens("clusterA", "serviceA1", pathScheme.toPathToken(reign.getCanonicalId()));
        String path = pathScheme.getAbsolutePath(PathType.PRESENCE, nodePath);
        assertTrue(reign.getZkClient().exists(path, false) == null);

        presenceService.show("clusterA", "serviceA1");

        Thread.sleep(1000);

        assertTrue(reign.getZkClient().exists(path, false) != null);
    }
}
