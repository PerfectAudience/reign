package io.reign.conf;

import static org.junit.Assert.assertTrue;
import io.reign.MasterTestSuite;
import io.reign.ObserverManager;
import io.reign.PathType;
import io.reign.ReignContext;
import io.reign.util.Structs;

import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author ypai
 * 
 */
public class UpdatingConfTest {
	private ConfService confService;

	@Before
	public void setUp() throws Exception {
		confService = MasterTestSuite.getReign().getService("conf");
	}

	@Test
	public void testBasic() throws Exception {
		UpdatingConf conf = null;
		try {
			ReignContext context = MasterTestSuite.getReign().getContext();

			// create self-updating Conf object, should be empty initially
			conf = new UpdatingConf("test", "service1/test1.conf", context);
			assertTrue(conf.get("key1") == null);
			assertTrue(conf.size() == 0);

			// edit configuration, change should be reflected in some period of
			// time
			confService.putConf(
					"test",
					"service1/test1.conf",
					Structs.<String, String> map().kv("key1", "value1")
							.kv("key2", "value2"));

			// wait up to 30 seconds total for updates
			for (int i = 0; i < 10; i++) {
				Thread.sleep(3000);
				if (conf.size() == 2) {
					break;
				}
			}

			assertTrue(conf.size() == 2);
			assertTrue(conf.get("key1").equals("value1")
					&& conf.get("key2").equals("value2"));
		} finally {
			conf.destroy();
		}
	}

	@Test
	public void testDestroy() throws Exception {

		ReignContext context = MasterTestSuite.getReign().getContext();
		ObserverManager observerManager = context.getObserverManager();
		String path = context.getPathScheme().getAbsolutePath(PathType.CONF,
				"test", "service1/test2.conf");

		// create self-updating Conf object, observer should be registered
		UpdatingConf conf = new UpdatingConf("test", "service1/test2.conf",
				context);
		assertTrue(observerManager.getReadOnlyObserverSet(path).size() == 1);

		// destroy object, observer should be removed
		conf.destroy();
		assertTrue(observerManager.getReadOnlyObserverSet(path).size() == 0);

	}
}
