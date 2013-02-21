package org.kompany.overlord.presence;

import java.util.Collections;
import java.util.Map;

import org.kompany.overlord.CanonicalNodeId;
import org.kompany.overlord.CanonicalServiceId;

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

    public CanonicalServiceId getCanonicalServiceId() {
        return new CanonicalServiceId(clusterId, serviceId);
    }

    public CanonicalNodeId getCanonicalNodeId() {
        return new CanonicalNodeId(clusterId, serviceId, nodeId);
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
        // TODO Auto-generated method stub
        return super.hashCode();
    }

}
