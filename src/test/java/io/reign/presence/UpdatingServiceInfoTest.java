package io.reign.presence;

import static org.junit.Assert.assertTrue;
import io.reign.MasterTestSuite;
import io.reign.ObserverManager;
import io.reign.PathScheme;
import io.reign.PathType;
import io.reign.ReignContext;

import org.junit.Before;
import org.junit.Test;

public class UpdatingServiceInfoTest {

	private PresenceService presenceService;

	@Before
	public void setUp() throws Exception {
		presenceService = MasterTestSuite.getReign().getService("presence");
	}

	@Test
	public void testBasic() throws Exception {
		UpdatingServiceInfo serviceInfo = null;
		try {
			ReignContext context = MasterTestSuite.getReign().getContext();
			PathScheme pathScheme = context.getPathScheme();

			// create self-updating Conf object, should be empty initially
			serviceInfo = new UpdatingServiceInfo("clusterA", "serviceA", context);
			assertTrue("clusterA".equals(serviceInfo.getClusterId()));
			assertTrue("serviceA".equals(serviceInfo.getServiceId()));
			assertTrue(serviceInfo.getNodeIdList().size() == 0);

			// edit configuration, change should be reflected in some period of
			// time
			presenceService.announce("clusterA", "serviceA", true);

			// wait up to 30 seconds total for updates
			for (int i = 0; i < presenceService.getHeartbeatIntervalMillis() / 1000 * 4; i++) {
				if (serviceInfo.getNodeIdList().size() == 1) {
					break;
				}
				Thread.sleep(1000);
			}

			assertTrue("clusterA".equals(serviceInfo.getClusterId()));
			assertTrue("serviceA".equals(serviceInfo.getServiceId()));
			assertTrue(serviceInfo.getNodeIdList().size() == 1);
			assertTrue(serviceInfo.getNodeIdList().get(0).equals(context.getNodeId()));
		} finally {
			serviceInfo.destroy();
		}
	}

	@Test
	public void testDestroy() throws Exception {
		UpdatingServiceInfo serviceInfo = null;

		ReignContext context = MasterTestSuite.getReign().getContext();
		ObserverManager observerManager = context.getObserverManager();
		String path = context.getPathScheme().getAbsolutePath(PathType.PRESENCE, "clusterA", "serviceB");

		// create self-updating Conf object, observer should be registered
		serviceInfo = new UpdatingServiceInfo("clusterA", "serviceB", context);
		assertTrue(observerManager.getReadOnlyObserverSet(path).size() == 1);

		// destroy object, observer should be removed
		serviceInfo.destroy();
		assertTrue(observerManager.getReadOnlyObserverSet(path).size() == 0);

	}
}