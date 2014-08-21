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

package io.reign.util.di;

import io.reign.Reign;
import io.reign.ReignException;
import io.reign.ReignMaker;

import java.util.Collections;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convenience class to facilitate Spring integration with default core services configured.
 * 
 * @author ypai
 * 
 */
public class SpringReignMaker extends ReignMaker {

	private static final Logger logger = LoggerFactory.getLogger(SpringReignMaker.class);

	private volatile Reign reign;

	private boolean testMode = false;

	private Map<String, Object> attributeMap = Collections.EMPTY_MAP;

	public SpringReignMaker() {
		super();
	}

	public Map<String, Object> getAttributeMap() {
		return attributeMap;
	}

	public <T> T getAttribute(String attributeKey) {
		return (T) attributeMap.get(attributeKey);
	}

	public void setAttributeMap(Map<String, Object> attributeMap) {
		this.attributeMap = attributeMap;
	}

	public void setFindPortAutomatically(boolean findPortAutomatically) {
		this.findPortAutomatically(findPortAutomatically);
	}

	public void setStartHook(Runnable startHook) {
		this.startHook(startHook);
	}

	public void setStopHook(Runnable stopHook) {
		this.stopHook(stopHook);
	}

	public void setFrameworkBasePath(String frameworkBasePath) {
		this.frameworkBasePath(frameworkBasePath);
	}

	public void setFrameworkClusterId(String frameworkClusterId) {
		this.frameworkClusterId(frameworkClusterId);
	}

	public void setTestMode(boolean testMode) {
		this.testMode = testMode;
	}

	public void setZkConnectString(String zkConnectString) {
		this.zkConnectString(zkConnectString);
	}

	public void setZkSessionTimeout(int zkSessionTimeout) {
		this.zkSessionTimeout(zkSessionTimeout);
	}

	public void setMessagingPort(Integer messagingPort) {
		this.messagingPort(messagingPort);
	}

	@Override
	public Reign get() {
		if (reign == null) {
			synchronized (this) {
				while (reign == null) {
					try {
						logger.info("Waiting for initialization...");
						this.wait(5000);
					} catch (InterruptedException e) {
						logger.warn("Interrupted while waiting for initialization", e);
					}
				} // while
			}
		}
		return reign;
	}

	/**
	 * Call using Spring "init-method" when initializing bean. Creates Reign object but does not start.
	 */
	public void init() {
		if (this.testMode) {
			if (!this.zkConnectString().startsWith("localhost:")) {
				throw new ReignException("When running in test mode, zkConnectString must reference localhost!");
			}
			this.zkClientTestMode(zkPort(this.zkConnectString()), this.zkSessionTimeout());
		}
		reign = super.get();
		synchronized (this) {
			logger.info("Initialized:  notifying all waiters...");
			this.notifyAll();
		}
	}

	/**
	 * Call using Spring "init-method" when initializing bean. Creates Reign object AND starts it up.
	 */
	public void initStart() {
		init();
		reign.start();
	}

	/**
	 * Call using Spring "destroy-method" when initializing bean.
	 */
	public void destroy() {
		reign.stop();
	}

	static int zkPort(String zkConnectString) {
		int colonIndex = zkConnectString.lastIndexOf(":");
		return Integer.parseInt(zkConnectString.substring(colonIndex + 1));

	}
}
