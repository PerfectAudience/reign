package org.kompany.overlord.presence;

import java.util.List;

/**
 * 
 * @author ypai
 * 
 */
public class ServiceInfo {

    private String clusterId;

    private String serviceId;

    private List<String> nodeIdList;

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public String getServiceId() {
        return serviceId;
    }

    public void setServiceId(String serviceId) {
        this.serviceId = serviceId;
    }

    public List<String> getNodeIdList() {
        return nodeIdList;
    }

    public void setNodeIdList(List<String> nodeIdList) {
        this.nodeIdList = nodeIdList;
    }

}
