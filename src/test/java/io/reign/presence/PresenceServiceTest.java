package io.reign.presence;

import static org.junit.Assert.*;

import java.util.List;

import io.reign.CanonicalId;
import io.reign.DefaultCanonicalId;
import io.reign.DefaultPathScheme;
import io.reign.MockZkClient;
import io.reign.PathScheme;
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

    private PathScheme pathScheme;
    private ZkClient zkClient;
    private PathCache pathCache;
    private PresenceService presenceService;

    @Before
    public void setUp() throws Exception {
        pathScheme = new DefaultPathScheme("/reign", "reign");
        zkClient = new MockZkClient();
        pathCache = new NullPathCache();

        presenceService = new PresenceService();
        presenceService.setZkClient(zkClient);
        presenceService.setPathScheme(pathScheme);
        presenceService.setPathCache(pathCache);
        presenceService.setContext(new ReignContext() {
            @Override
            public <T extends Service> T getService(String serviceName) {
                return null;
            }

            @Override
            public CanonicalId getCanonicalId() {
                return new DefaultCanonicalId("pid123", "1.1.1.1", "test.reign.io", 80, 33033);
            }

            @Override
            public ZkClient getZkClient() {
                return zkClient;
            }

            @Override
            public PathScheme getPathScheme() {
                return pathScheme;
            }

            @Override
            public List<ACL> getDefaultZkAclList() {
                return Reign.DEFAULT_ACL_LIST;
            }

            @Override
            public PathCache getPathCache() {
                return pathCache;
            }
        });

        zkClient.register(presenceService);

        presenceService.init();

        // add some services
        presenceService.announce("clusterA", "serviceA", true);
        presenceService.announce("clusterA", "serviceB", true);
        presenceService.announce("clusterB", "serviceC", true);
        presenceService.announce("clusterB", "serviceD", false);

    }

    @After
    public void tearDown() throws Exception {
        presenceService.destroy();
    }

    @Test
    public void testLookupClusters() {
        // announcements are processed asynchronously, so wait until services are available
        presenceService.waitUntilAvailable("clusterA", "serviceA", -1);

        List<String> clusterIdList = presenceService.lookupClusters();
        assertTrue("Should be 2 but got " + clusterIdList.size(), clusterIdList.size() == 2);
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
        // fail("Not yet implemented");
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
    public void testHideStringString() {
        // fail("Not yet implemented");
    }

    @Test
    public void testShowStringString() {
        // fail("Not yet implemented");
    }

}
