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

import java.util.Collections;
import java.util.Map;

/**
 * 
 * @author ypai
 * 
 */
public class NodeInfo {

    private final String clusterId;

    private final String serviceId;

    private final String nodeId;

    private Map<String, String> attributeMap;

    public NodeInfo(String clusterId, String serviceId, String nodeId, Map<String, String> attributeMap) {
        if (clusterId == null || serviceId == null || nodeId == null) {
            throw new IllegalArgumentException("clusterId, serviceId, and/or nodeId cannot be null!");
        }

        this.clusterId = clusterId;
        this.serviceId = serviceId;
        this.nodeId = nodeId;

        if (attributeMap != null) {
            this.attributeMap = attributeMap;
        } else {
            this.attributeMap = Collections.EMPTY_MAP;
        }
    }

    public String getClusterId() {
        return clusterId;
    }

    public String getServiceId() {
        return serviceId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public Object getAttribute(String key) {
        return attributeMap.get(key);
    }

    public Map<String, String> getAttributeMap() {
        return Collections.unmodifiableMap(attributeMap);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || !(obj instanceof NodeInfo)) {
            return false;
        }

        // compare map sizes
        NodeInfo ni = (NodeInfo) obj;
        if (this.attributeMap.size() != ni.getAttributeMap().size()) {
            return false;
        }

        // compare cluster/service path values
        if (!this.getClusterId().equals(ni.getClusterId()) || !this.getServiceId().equals(ni.getServiceId())
                || !this.getNodeId().equals(ni.getNodeId())) {
            return false;
        }

        // iterate through map keys and make sure all values are the same
        for (String key : this.attributeMap.keySet()) {
            if (!this.attributeMap.get(key).equals(ni.getAttribute(key))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        // TODO possibly do something more clever later

        return super.hashCode();
    }

}
