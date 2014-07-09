/*
 Copyright 2014 Yen Pai ypai@reign.io

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

import io.reign.PathType;
import io.reign.ReignContext;

import java.util.List;

/**
 * 
 * @author ypai
 * 
 */
public class UpdatingServiceInfo implements ServiceInfo {

    private volatile ServiceInfo serviceInfo;
    private final ReignContext context;
    private final String clusterId;
    private final String serviceId;
    private final PresenceObserver<ServiceInfo> observer;

    public UpdatingServiceInfo(String clusterId, String serviceId, ReignContext context) {
        if (clusterId == null || serviceId == null) {
            throw new IllegalArgumentException("clusterId and/or serviceId cannot be null!");
        }

        this.clusterId = clusterId;
        this.serviceId = serviceId;
        this.context = context;
        this.observer = new PresenceObserver<ServiceInfo>() {
            @Override
            public void updated(ServiceInfo updated, ServiceInfo previous) {
                serviceInfo = updated;
            }
        };

        PresenceService presenceService = context.getService("presence");
        serviceInfo = presenceService.getServiceInfo(clusterId, serviceId, observer);

    }

    public void destroy() {
        String path = context.getPathScheme().getAbsolutePath(PathType.PRESENCE, clusterId, serviceId);
        context.getObserverManager().remove(path, observer);
    }

    public String getClusterId() {
        return clusterId;
    }

    public String getServiceId() {
        return serviceId;
    }

    public List<String> getNodeIdList() {
        return serviceInfo.getNodeIdList();
    }

}
