/*
 Copyright 2013 Yen Pai ypai@reign.io

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package io.reign;

import io.reign.DefaultNodeIdProvider.DefaultNodeId;
import io.reign.util.JacksonUtil;

/**
 * 
 * @author ypai
 * 
 */
public class StaticNodeInfo implements NodeInfo, NodeAddress {

	private final DefaultNodeId defaultNodeId;

	public StaticNodeInfo(String nodeId) {
		if (nodeId == null) {
			throw new IllegalArgumentException("nodeId cannot be null!");
		}

		try {
			defaultNodeId = JacksonUtil.getObjectMapper().readValue(nodeId, DefaultNodeId.class);

		} catch (Exception e) {
			throw new ReignException("" + e, e);
		}

	}

	public StaticNodeInfo(String processId, String ipAddress, String host, Integer messagingPort) {
		defaultNodeId = new DefaultNodeId(processId, ipAddress, host, messagingPort);
	}

	@Override
	public String getNodeId() {
		return defaultNodeId.toString();
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
