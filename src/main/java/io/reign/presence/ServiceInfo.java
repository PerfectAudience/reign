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
import java.util.List;

import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * 
 * @author ypai
 * 
 */
public class ServiceInfo {

    private final String clusterId;

    private final String serviceId;

    @JsonSerialize(using = NodeIdListSerializer.class)
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

}
