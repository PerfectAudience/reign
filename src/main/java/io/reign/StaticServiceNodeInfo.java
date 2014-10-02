/*
 * Copyright 2013, 2014 Yen Pai ypai@kompany.org
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

import io.reign.DefaultNodeIdProvider.DefaultNodeId;
import io.reign.util.JacksonUtil;

import java.util.Collections;
import java.util.Map;

/**
 * 
 * @author ypai
 *
 */
public class StaticServiceNodeInfo implements ServiceNodeInfo, NodeAddress {
	private Map<String, String> attributeMap;
	private final String clusterId;
	private final String serviceId;
	private final String nodeId;

	private final DefaultNodeId defaultNodeId;

	public StaticServiceNodeInfo(String clusterId, String serviceId, String nodeId, Map<String, String> attributeMap) {
		if (clusterId == null && serviceId == null && nodeId == null) {
			throw new IllegalArgumentException("clusterId, serviceId, nodeId cannot be null!");
		}

		this.clusterId = clusterId;
		this.serviceId = serviceId;
		this.nodeId = nodeId;

		try {
			defaultNodeId = JacksonUtil.getObjectMapper().readValue(nodeId, DefaultNodeId.class);

			if (attributeMap != null) {
				this.attributeMap = Collections.unmodifiableMap(attributeMap);
			} else {
				this.attributeMap = Collections.EMPTY_MAP;
			}
		} catch (Exception e) {
			throw new ReignException("" + e, e);
		}
	}

	@Override
	public Object getAttribute(String key) {
		return attributeMap.get(key);
	}

	@Override
	public Map<String, String> getAttributeMap() {
		return attributeMap;
	}

	@Override
	public String getClusterId() {
		return clusterId;
	}

	@Override
	public String getServiceId() {
		return serviceId;
	}

	@Override
	public String getNodeId() {
		return nodeId;
	}

	@Override
	public String getProcessId() {
		return defaultNodeId.getProcessId();
	}

	@Override
	public String getIpAddress() {
		return defaultNodeId.getIpAddress();
	}

	@Override
	public String getHost() {
		return defaultNodeId.getHost();
	}

	@Override
	public Integer getMessagingPort() {
		return defaultNodeId.getMessagingPort();
	}

}
