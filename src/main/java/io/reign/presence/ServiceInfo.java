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

    // @Override
    // public boolean equals(Object obj) {
    // if (this == obj) {
    // return true;
    // }
    //
    // if (obj == null || !(obj instanceof ServiceInfo)) {
    // return false;
    // }
    //
    // // compare child list sizes
    // ServiceInfo si = (ServiceInfo) obj;
    // if (this.getNodeIdList().size() != si.getNodeIdList().size()) {
    // return false;
    // }
    //
    // // compare cluster/service path values
    // if (!this.getClusterId().equals(si.getClusterId()) || !this.getServiceId().equals(si.getServiceId())) {
    // return false;
    // }
    //
    // // sort child lists and then compare individual child list values
    // List<String> tmpThisNodeList = new ArrayList<String>(this.nodeIdList.size());
    // tmpThisNodeList.addAll(this.nodeIdList);
    // Collections.sort(tmpThisNodeList);
    //
    // List<String> tmpThatNodeList = new ArrayList<String>(si.getNodeIdList().size());
    // tmpThatNodeList.addAll(si.getNodeIdList());
    // Collections.sort(tmpThatNodeList);
    //
    // for (int i = 0; i < tmpThisNodeList.size(); i++) {
    // if (!tmpThisNodeList.get(i).equals(tmpThatNodeList.get(i))) {
    // return false;
    // }
    // }
    //
    // return true;
    // }
    //
    // @Override
    // public int hashCode() {
    // // TODO Auto-generated method stub
    // return super.hashCode();
    // }

}
