package io.reign.presence;

import static org.junit.Assert.assertTrue;
import io.reign.MasterTestSuite;
import io.reign.ObserverManager;
import io.reign.PathScheme;
import io.reign.PathType;
import io.reign.ReignContext;

import org.junit.Before;
import org.junit.Test;

public class UpdatingNodeInfoTest {

	private PresenceService presenceService;

	@Before
	public void setUp() throws Exception {
		presenceService = MasterTestSuite.getReign().getService("presence");
	}

	@Test
	public void testBasic() throws Exception {
		UpdatingNodeInfo nodeInfo = null;
		try {
			ReignContext context = MasterTestSuite.getReign().getContext();
			PathScheme pathScheme = context.getPathScheme();

			// create self-updating Conf object, should be empty initially
			nodeInfo = new UpdatingNodeInfo("clusterABC", "serviceABC",
					context.getNodeId(), context);
			assertTrue(nodeInfo.getClusterId() == null);
			assertTrue(nodeInfo.getServiceId() == null);
			assertTrue(nodeInfo.getNodeId() == null);
			assertTrue(nodeInfo.getAttribute("key1") == null);
			assertTrue(nodeInfo.getAttributeMap() == null);

			// edit configuration, change should be reflected in some period of
			// time
			presenceService.announce("clusterABC", "serviceABC", true);

			for (int i = 0; i < presenceService.getHeartbeatIntervalMillis() / 1000 * 4; i++) {
				if (nodeInfo.getNodeId() != null) {
					break;
				}
				Thread.sleep(1000);
			}

			assertTrue("clusterABC".equals(nodeInfo.getClusterId()));
			assertTrue("serviceABC".equals(nodeInfo.getServiceId()));
			assertTrue(nodeInfo.getNodeId().toString()
					.equals(context.getNodeId().toString()));

		} finally {
			nodeInfo.destroy();
		}
	}

	@Test
	public void testDestroy() throws Exception {
		UpdatingNodeInfo nodeInfo = null;

		ReignContext context = MasterTestSuite.getReign().getContext();
		ObserverManager observerManager = context.getObserverManager();
		String path = context.getPathScheme().getAbsolutePath(
				PathType.PRESENCE, "clusterA", "serviceB",
				context.getNodeId().toString());

		// create self-updating Conf object, observer should be registered
		nodeInfo = new UpdatingNodeInfo("clusterA", "serviceB",
				context.getNodeId(), context);
		assertTrue(observerManager.getReadOnlyObserverSet(path).size() == 1);

		// destroy object, observer should be removed
		nodeInfo.destroy();
		assertTrue(observerManager.getReadOnlyObserverSet(path).size() == 0);

	}
}