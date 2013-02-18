package org.kompany.overlord.presence;

import java.util.Collections;
import java.util.Map;

/**
 * 
 * @author ypai
 * 
 */
public class NodeInfo {

    private String clusterId;

    private String serviceId;

    private String nodeId;

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

        // compare child list sizes
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
        // TODO Auto-generated method stub
        return super.hashCode();
    }

}
