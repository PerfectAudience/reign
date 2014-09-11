/*
 * Copyright 2013 Yen Pai ypai@reign.io
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.reign;

import io.reign.conf.ConfService;
import io.reign.coord.CoordinationService;
import io.reign.data.DataService;
import io.reign.mesg.DefaultMessagingService;
import io.reign.mesg.MessagingService;
import io.reign.metrics.MetricsService;
import io.reign.presence.PresenceService;
import io.reign.util.PortUtil;
import io.reign.zk.PathCache;
import io.reign.zk.ResilientZkClient;
import io.reign.zk.SimplePathCache;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convenient way to quickly configure the main framework object.
 * 
 * @author ypai
 * 
 */
public class ReignMaker {

	private static final Logger logger = LoggerFactory.getLogger(ReignMaker.class);

	private String zkConnectString;
	private int zkSessionTimeout = 30000;

	private int pathCacheMaxSize = 1024;
	private int pathCacheMaxConcurrencyLevel = 2;

	private PathCache pathCache = null;
	private ZkClient zkClient = null;

	private PathScheme pathScheme = null;

	private String frameworkClusterId;

	private String frameworkBasePath;

	private Integer messagingPort = Reign.DEFAULT_MESSAGING_PORT;

	private NodeIdProvider canonicalIdMaker = null;

	private final Map<String, Service> serviceMap = new HashMap<String, Service>();

	private boolean startZkTestServer = false;
	private int zkTestServerPort = 22181;

	private Runnable startHook;
	private Runnable stopHook;

	private boolean findPortAutomatically = false;

	public ReignMaker startHook(Runnable startHook) {
		this.startHook = startHook;
		return this;
	}

	public ReignMaker stopHook(Runnable stopHook) {
		this.stopHook = stopHook;
		return this;
	}

	private ReignMaker core() {
		// configure Reign with all core services

		presence();

		conf();

		coord();

		data();

		mesg();

		metrics();

		nullService();

		return this;
	}

	private ReignMaker metrics() {
		MetricsService metricsService = new MetricsService();
		serviceMap.put("metrics", metricsService);

		return this;
	}

	private ReignMaker presence() {
		PresenceService presenceService = new PresenceService();
		serviceMap.put("presence", presenceService);

		// alternate route for more concise messaging
		serviceMap.put("P", presenceService);

		return this;
	}

	private ReignMaker nullService() {
		NullService nullService = new NullService();
		serviceMap.put("null", nullService);

		return this;
	}

	private ReignMaker conf() {
		ConfService confService = new ConfService();
		serviceMap.put("conf", confService);

		// alternate route for more concise messaging
		serviceMap.put("F", confService);

		return this;
	}

	private ReignMaker coord() {
		CoordinationService coordService = new CoordinationService();
		serviceMap.put("coord", coordService);

		// alternate route for more concise messaging
		serviceMap.put("C", coordService);

		return this;
	}

	private ReignMaker data() {
		DataService dataService = new DataService();
		serviceMap.put("data", dataService);

		// alternate route for more concise messaging
		serviceMap.put("D", dataService);

		return this;
	}

	private ReignMaker mesg() {
		MessagingService messagingService = new DefaultMessagingService();

		serviceMap.put("mesg", messagingService);

		if (findPortAutomatically) {
			messagingPort = Reign.DEFAULT_MESSAGING_PORT;
			while (!PortUtil.available(messagingPort)) {
				messagingPort++;
			}
		}

		if (messagingPort == null) {
			messagingPort = Reign.DEFAULT_MESSAGING_PORT;
			messagingService.setPort(messagingPort);
		} else {
			messagingService.setPort(messagingPort);
		}

		logger.info("Configuring messaging service:  port={}", messagingPort);

		// alternate route for more concise messaging
		serviceMap.put("M", messagingService);

		return this;
	}

	public ReignMaker findPortAutomatically(boolean findPortAutomatically) {
		this.findPortAutomatically = findPortAutomatically;
		return this;
	}

	public ReignMaker messagingPort(Integer messagingPort) {
		if (messagingPort == null) {
			throw new IllegalArgumentException("Invalid argument:  'messagingPort' cannot be null!");
		}
		this.messagingPort = messagingPort;

		// set messaging port if message service has already been configured
		MessagingService messagingService = (MessagingService) serviceMap.get("messaging");
		if (messagingService != null) {
			messagingService.setPort(messagingPort);
		}

		return this;
	}

	public ReignMaker canonicalIdMaker(NodeIdProvider canonicalIdMaker) {
		this.canonicalIdMaker = canonicalIdMaker;
		return this;
	}

	public ReignMaker zkClient(String zkConnectString, int zkSessionTimeout) {
		this.zkConnectString = zkConnectString;
		this.zkSessionTimeout = zkSessionTimeout;
		return this;

	}

	public ReignMaker zkConnectString(String zkConnectString) {
		this.zkConnectString = zkConnectString;
		return this;
	}

	public ReignMaker zkSessionTimeout(int zkSessionTimeout) {
		this.zkSessionTimeout = zkSessionTimeout;
		return this;
	}

	public String zkConnectString() {
		return this.zkConnectString;
	}

	public int zkSessionTimeout() {
		return this.zkSessionTimeout;
	}

	public ReignMaker startZkTestServer(boolean startZkTestServer) {
		this.startZkTestServer = startZkTestServer;
		return this;
	}

	public ReignMaker zkTestServerPort(int zkTestServerPort) {
		this.zkTestServerPort = zkTestServerPort;
		return this;
	}

	public TestingServer startZkTestServer(int zkPort) {

		logger.debug("Starting in-process ZooKeeper server on port {} -- meant for testing only!", zkPort);
		try {
			String dataDirectory = System.getProperty("java.io.tmpdir");
			if (!dataDirectory.endsWith("/")) {
				dataDirectory += File.separator;
			}
			dataDirectory += UUID.randomUUID().toString();

			logger.debug("ZK dataDirectory={}", dataDirectory);

			File dir = new File(dataDirectory, "zookeeper").getAbsoluteFile();

			return new TestingServer(zkPort, dir);

		} catch (Exception e) {
			logger.error("Trouble starting test ZooKeeper instance:  " + e, e);
		}

		return null;

	}

	public ReignMaker registerService(String serviceName, Service service) {
		serviceMap.put(serviceName, service);
		return this;
	}

	public ReignMaker pathCache(int maxSize, int concurrencyLevel) {
		this.pathCacheMaxSize = maxSize;
		this.pathCacheMaxConcurrencyLevel = concurrencyLevel;
		return this;
	}

	public ReignMaker pathCache(PathCache pathCache) {
		this.pathCache = pathCache;
		return this;
	}

	public ReignMaker zkClient(ZkClient zkClient) {
		this.zkClient = zkClient;
		return this;
	}

	public ReignMaker frameworkBasePath(String frameworkBasePath) {
		this.frameworkBasePath = frameworkBasePath;
		return this;
	}

	public ReignMaker frameworkClusterId(String frameworkClusterId) {
		this.frameworkClusterId = frameworkClusterId;
		return this;
	}

	public Reign get() {

		// start test zk server if configured
		TestingServer zkTestServer = null;
		if (this.startZkTestServer) {
			zkTestServer = this.startZkTestServer(this.zkTestServerPort);
		}

		Reign s = null;

		// always add core services
		core();

		// see if we need to set defaults
		if (frameworkClusterId == null) {
			frameworkClusterId = Reign.DEFAULT_FRAMEWORK_CLUSTER_ID;
		}
		if (frameworkBasePath == null) {
			frameworkBasePath = Reign.DEFAULT_FRAMEWORK_BASE_PATH;
		}
		if (pathCache == null) {
			pathCache = defaultPathCache();
		}
		if (zkClient == null) {
			zkClient = defaultZkClient(pathCache);
		}
		if (pathScheme == null) {
			pathScheme = defaultPathScheme(frameworkBasePath, frameworkClusterId);
		}
		if (canonicalIdMaker == null) {
			canonicalIdMaker = defaultCanonicalIdMaker();
		}

		// build
		s = new Reign(zkClient, pathScheme, canonicalIdMaker, zkTestServer);
		s.registerServices(serviceMap);
		s.setStartHook(startHook);
		s.setStopHook(stopHook);

		return s;
	}

	ZkClient defaultZkClient(PathCache pathCache) {
		if (zkConnectString == null || zkSessionTimeout <= 0) {
			throw new ReignException("zkConnectString and zkSessionTimeout not configured!");
		}

		ZkClient zkClient = null;
		try {
			zkClient = new ResilientZkClient(zkConnectString, zkSessionTimeout);
		} catch (IOException e) {
			throw new ReignException("Fatal error:  could not initialize Zookeeper client!", e);
		}
		return zkClient;
	}

	PathCache defaultPathCache() {
		if (pathCacheMaxSize < 1 || pathCacheMaxConcurrencyLevel < 1) {
			throw new ReignException(
			        "zkClient, pathCacheMaxSize, and pathCacheMaxConcurrencyLevel must be configured to create default path cache!");
		}

		return new SimplePathCache(this.pathCacheMaxSize, this.pathCacheMaxConcurrencyLevel);
	}

	PathScheme defaultPathScheme(String basePath, String frameworkClusterId) {
		return new DefaultPathScheme(basePath, frameworkClusterId);
	}

	NodeIdProvider defaultCanonicalIdMaker() {
		DefaultNodeIdProvider idMaker = new DefaultNodeIdProvider(messagingPort);
		return idMaker;
	}

}
