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

    private Map<String, Object> attributeMap;

    public NodeInfo(String clusterId, String serviceId, String nodeId, Map<String, Object> attributeMap) {
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

    // public void setClusterId(String clusterId) {
    // this.clusterId = clusterId;
    // }

    public String getServiceId() {
        return serviceId;
    }

    // public void setServiceId(String serviceId) {
    // this.serviceId = serviceId;
    // }

    public String getNodeId() {
        return nodeId;
    }

    // public void setNodeId(String nodeId) {
    // this.nodeId = nodeId;
    // }

    public Object getAttribute(String key) {
        return attributeMap.get(key);
    }

    public Map<String, Object> getAttributeMap() {
        return Collections.unmodifiableMap(attributeMap);
    }

    // public void setAttribute(String key, String value) {
    // attributeMap.put(key, value);
    // }

}
