package io.reign.presence;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 
 * @author ypai
 * 
 */
public class ServiceInfo {

    private final String clusterId;

    private final String serviceId;

    private List<String> nodeIdList;

    public ServiceInfo(String clusterId, String serviceId, List<String> nodeIdList) {
        if (clusterId == null || serviceId == null) {
            throw new IllegalArgumentException("clusterId and/or serviceId cannot be null!");
        }

        this.clusterId = clusterId;
        this.serviceId = serviceId;

        if (nodeIdList != null) {
            this.nodeIdList = nodeIdList;
        } else {
            this.nodeIdList = Collections.EMPTY_LIST;
        }
    }

    public String getClusterId() {
        return clusterId;
    }

    public String getServiceId() {
        return serviceId;
    }

    public List<String> getNodeIdList() {
        return Collections.unmodifiableList(nodeIdList);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || !(obj instanceof ServiceInfo)) {
            return false;
        }

        // compare child list sizes
        ServiceInfo si = (ServiceInfo) obj;
        if (this.getNodeIdList().size() != si.getNodeIdList().size()) {
            return false;
        }

        // compare cluster/service path values
        if (!this.getClusterId().equals(si.getClusterId()) || !this.getServiceId().equals(si.getServiceId())) {
            return false;
        }

        // sort child lists and then compare individual child list values
        List<String> tmpThisNodeList = new ArrayList<String>(this.nodeIdList.size());
        tmpThisNodeList.addAll(this.nodeIdList);
        Collections.sort(tmpThisNodeList);

        List<String> tmpThatNodeList = new ArrayList<String>(si.getNodeIdList().size());
        tmpThatNodeList.addAll(si.getNodeIdList());
        Collections.sort(tmpThatNodeList);

        for (int i = 0; i < tmpThisNodeList.size(); i++) {
            if (!tmpThisNodeList.get(i).equals(tmpThatNodeList.get(i))) {
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
