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

import io.reign.NodeId;
import io.reign.PathType;
import io.reign.ReignContext;

import java.util.Map;

/**
 * 
 * @author ypai
 * 
 */
public class UpdatingNodeInfo implements NodeInfo {

	private volatile NodeInfo nodeInfo;
	private final String clusterId;
	private final String serviceId;
	private NodeId nodeId;
	private final ReignContext context;
	private final PresenceObserver<NodeInfo> observer;

	public UpdatingNodeInfo(String clusterId, String serviceId, NodeId nodeId,
			ReignContext context) {
		if (clusterId == null && serviceId == null && nodeId == null) {
			throw new IllegalArgumentException(
					"clusterId, serviceId, nodeId cannot be null!");
		}

		this.clusterId = clusterId;
		this.serviceId = serviceId;
		this.nodeId = nodeId;
		this.context = context;
		this.observer = new PresenceObserver<NodeInfo>() {
			@Override
			public void updated(NodeInfo updated, NodeInfo previous) {
				nodeInfo = updated;
			}
		};

		PresenceService presenceService = context.getService("presence");
		nodeInfo = presenceService.getNodeInfo(clusterId, serviceId, context
				.getPathScheme().toPathToken(nodeId), observer);
	}

	public void destroy() {
		String path = context.getPathScheme().getAbsolutePath(
				PathType.PRESENCE, clusterId, serviceId,
				context.getPathScheme().toPathToken(nodeId));
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
		if (nodeInfo == null) {
			return null;
		}
		return nodeInfo.getClusterId();
	}

	public String getServiceId() {
		if (nodeInfo == null) {
			return null;
		}
		return nodeInfo.getServiceId();
	}

	public NodeId getNodeId() {
		if (nodeInfo == null) {
			return null;
		}
		return nodeInfo.getNodeId();
	}
}
