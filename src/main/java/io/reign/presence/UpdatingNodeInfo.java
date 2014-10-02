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

package io.reign.presence;

import io.reign.PathType;
import io.reign.ReignContext;
import io.reign.ServiceNodeInfo;

import java.util.Map;

/**
 * 
 * @author ypai
 * 
 */
public class UpdatingNodeInfo implements ServiceNodeInfo {

	private volatile ServiceNodeInfo nodeInfo;
	private final String clusterId;
	private final String serviceId;
	private final String nodeId;
	private final ReignContext context;
	private final PresenceObserver<ServiceNodeInfo> observer;

	public UpdatingNodeInfo(String clusterId, String serviceId, String nodeId, ReignContext context) {
		if (clusterId == null && serviceId == null && nodeId == null) {
			throw new IllegalArgumentException("clusterId, serviceId, nodeId cannot be null!");
		}

		this.clusterId = clusterId;
		this.serviceId = serviceId;
		this.nodeId = nodeId;

		this.context = context;
		this.observer = new PresenceObserver<ServiceNodeInfo>() {
			@Override
			public void updated(ServiceNodeInfo updated, ServiceNodeInfo previous) {
				nodeInfo = updated;
			}
		};

		PresenceService presenceService = context.getService("presence");
		nodeInfo = presenceService.getNodeInfo(clusterId, serviceId, nodeId, observer);
	}

	public void destroy() {
		String path = context.getPathScheme().getAbsolutePath(PathType.PRESENCE, clusterId, serviceId, nodeId);
		context.getObserverManager().remove(path, observer);
	}

	public Object getAttribute(String key) {
		if (nodeInfo == null) {
			return null;
		}
		return nodeInfo.getAttribute(key);
	}

	public Map<String, String> getAttributeMap() {
		if (nodeInfo == null) {
			return null;
		}
		return nodeInfo.getAttributeMap();
	}

	public String getClusterId() {
		return clusterId;
	}

	public String getServiceId() {
		return serviceId;
	}

	@Override
	public String getNodeId() {
		return nodeId;
	}

	@Override
	public String getProcessId() {
		if (nodeInfo == null) {
			return null;
		}
		return nodeInfo.getProcessId();
	}

	@Override
	public String getIpAddress() {
		if (nodeInfo == null) {
			return null;
		}
		return nodeInfo.getIpAddress();
	}

	@Override
	public String getHost() {
		if (nodeInfo == null) {
			return null;
		}
		return nodeInfo.getHost();
	}

	@Override
	public Integer getMessagingPort() {
		if (nodeInfo == null) {
			return null;
		}
		return nodeInfo.getMessagingPort();
	}

}
