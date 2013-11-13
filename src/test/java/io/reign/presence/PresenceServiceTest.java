package io.reign.presence;

import static org.junit.Assert.*;

import java.util.List;

import io.reign.CanonicalId;
import io.reign.DefaultCanonicalId;
import io.reign.DefaultPathScheme;
import io.reign.MasterTestSuite;
import io.reign.MockZkClient;
import io.reign.PathScheme;
import io.reign.PathType;
import io.reign.Reign;
import io.reign.ReignContext;
import io.reign.Service;
import io.reign.ZkClient;
import io.reign.util.NullPathCache;
import io.reign.util.PathCache;

import org.apache.zookeeper.data.ACL;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PresenceServiceTest {

    private PresenceService presenceService;

    @Before
    public void setUp() throws Exception {

        presenceService = MasterTestSuite.getReign().getService("presence");

        // add some services
        presenceService.announce("clusterA", "serviceA", true);
        presenceService.announce("clusterA", "serviceB", true);
        presenceService.announce("clusterB", "serviceC", true);
        presenceService.announce("clusterB", "serviceD", false);

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testLookupClusters() {
        // announcements are processed asynchronously, so wait until services are available
        presenceService.waitUntilAvailable("clusterA", "serviceA", -1);

        List<String> clusterIdList = presenceService.lookupClusters();

        // should be 3 clusters, including the default "reign" cluster
        assertTrue("Should be 3 but got " + clusterIdList.size() + ":  " + clusterIdList, clusterIdList.size() == 3);
        assertTrue(clusterIdList.contains("clusterA"));
        assertTrue(clusterIdList.contains("clusterB"));
    }

    @Test
    public void testLookupServices() {
        presenceService.waitUntilAvailable("clusterA", "serviceA", -1);

        List<String> serviceIdList = presenceService.lookupServices("clusterA");
        assertTrue("Should be 2 but got " + serviceIdList.size(), serviceIdList.size() == 2);
        assertTrue(serviceIdList.contains("serviceA"));
        assertTrue(serviceIdList.contains("serviceB"));
    }

    @Test
    public void testObserveStringStringSimplePresenceObserverOfServiceInfo() {
        // fail("Not yet implemented");
    }

    @Test
    public void testObserveStringStringStringSimplePresenceObserverOfNodeInfo() {
        // fail("Not yet implemented");
    }

    @Test
    public void testWaitUntilAvailableStringStringLong() {
        // fail("Not yet implemented");
    }

    @Test
    public void testWaitUntilAvailableStringStringSimplePresenceObserverOfServiceInfoLong() {
        // fail("Not yet implemented");
    }

    @Test
    public void testWaitUntilAvailableStringStringSimplePresenceObserverOfServiceInfoDataSerializerOfMapOfStringStringBooleanLong() {
        // fail("Not yet implemented");
    }

    @Test
    public void testLookupServiceInfoStringString() {
        // fail("Not yet implemented");
    }

    @Test
    public void testLookupServiceInfoStringStringSimplePresenceObserverOfServiceInfo() {
        // fail("Not yet implemented");
    }

    @Test
    public void testLookupServiceInfoStringStringSimplePresenceObserverOfServiceInfoDataSerializerOfMapOfStringStringBoolean() {
        // fail("Not yet implemented");
    }

    @Test
    public void testWaitUntilAvailableStringStringStringLong() {
        // fail("Not yet implemented");
    }

    @Test
    public void testWaitUntilAvailableStringStringStringSimplePresenceObserverOfNodeInfoLong() {
        // fail("Not yet implemented");
    }

    @Test
    public void testWaitUntilAvailableStringStringStringSimplePresenceObserverOfNodeInfoDataSerializerOfMapOfStringStringBooleanLong() {
        // fail("Not yet implemented");
    }

    @Test
    public void testLookupNodeInfoStringStringString() {
        Reign reign = MasterTestSuite.getReign();
        NodeInfo nodeInfo = presenceService.lookupNodeInfo("clusterA", "serviceA", reign.getContext()
                .getCanonicalIdPathToken());
        assertTrue("clusterA".equals(nodeInfo.getClusterId()));
        assertTrue("serviceA".equals(nodeInfo.getServiceId()));
        assertTrue(reign.getPathScheme().toPathToken(reign.getCanonicalId()).equals(nodeInfo.getNodeId()));
    }

    @Test
    public void testLookupNodeInfoStringStringStringSimplePresenceObserverOfNodeInfo() {
        // fail("Not yet implemented");
    }

    @Test
    public void testLookupNodeInfoStringStringStringSimplePresenceObserverOfNodeInfoDataSerializerOfMapOfStringStringBoolean() {
        // fail("Not yet implemented");
    }

    @Test
    public void testAnnounceStringStringBoolean() {
        // fail("Not yet implemented");
    }

    @Test
    public void testAnnounceStringStringBooleanMapOfStringString() {
        // fail("Not yet implemented");
    }

    @Test
    public void testAnnounceStringStringBooleanListOfACL() {
        // fail("Not yet implemented");
    }

    @Test
    public void testAnnounceStringStringBooleanMapOfStringStringListOfACL() {
        // fail("Not yet implemented");
    }

    @Test
    public void testHideStringString() throws Exception {
        presenceService.hide("clusterA", "serviceA");

        Thread.sleep(1000);

        Reign reign = MasterTestSuite.getReign();
        PathScheme pathScheme = reign.getPathScheme();
        String nodePath = pathScheme.joinTokens("clusterA", "serviceA", pathScheme.toPathToken(reign.getCanonicalId()));
        String path = pathScheme.getAbsolutePath(PathType.PRESENCE, nodePath);
        assertTrue(reign.getZkClient().exists(path, false) == null);
    }

    @Test
    public void testShowStringString() throws Exception {
        presenceService.hide("clusterA", "serviceA");

        Thread.sleep(1000);

        Reign reign = MasterTestSuite.getReign();
        PathScheme pathScheme = reign.getPathScheme();
        String nodePath = pathScheme.joinTokens("clusterA", "serviceA", pathScheme.toPathToken(reign.getCanonicalId()));
        String path = pathScheme.getAbsolutePath(PathType.PRESENCE, nodePath);
        assertTrue(reign.getZkClient().exists(path, false) == null);

        presenceService.show("clusterA", "serviceA");

        Thread.sleep(1000);

        assertTrue(reign.getZkClient().exists(path, false) != null);
    }

}
