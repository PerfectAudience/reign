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

import java.util.Collections;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonRawValue;

/**
 * 
 * @author ypai
 * 
 */
public class StaticNodeInfo implements NodeInfo {

    private Map<String, String> attributeMap;
    private final String clusterId;
    private final String serviceId;

    @JsonRawValue
    private final NodeId nodeId;

    public StaticNodeInfo(String clusterId, String serviceId, NodeId nodeId, Map<String, String> attributeMap) {
        if (clusterId == null && serviceId == null && nodeId == null) {
            throw new IllegalArgumentException("clusterId, serviceId, nodeId cannot be null!");
        }

        this.clusterId = clusterId;
        this.serviceId = serviceId;
        this.nodeId = nodeId;

        if (attributeMap != null) {
            this.attributeMap = Collections.unmodifiableMap(attributeMap);
        } else {
            this.attributeMap = Collections.EMPTY_MAP;
        }
    }

    public Object getAttribute(String key) {
        return attributeMap.get(key);
    }

    public Map<String, String> getAttributeMap() {
        return attributeMap;
    }

    public String getClusterId() {
        return clusterId;
    }

    public String getServiceId() {
        return serviceId;
    }

    public NodeId getNodeId() {
        return nodeId;
    }
}
