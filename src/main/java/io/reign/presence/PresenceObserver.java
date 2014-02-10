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

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reign.AbstractObserver;
import io.reign.DataSerializer;
import io.reign.JsonDataSerializer;
import io.reign.Observer;
import io.reign.ObserverManager;

/**
 * 
 * @author ypai
 * 
 * @param <T>
 */
public abstract class PresenceObserver<T> extends AbstractObserver {

    private static final DataSerializer<Map<String, String>> nodeAttributeSerializer = new JsonDataSerializer<Map<String, String>>();

    private String clusterId = null;
    private String serviceId = null;
    private String nodeId = null;

    public abstract void updated(T updated);

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public void setServiceId(String serviceId) {
        this.serviceId = serviceId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
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

    @Override
    public void nodeChildrenChanged(List<String> updatedChildList) {
        if (serviceId != null) {
            ServiceInfo updated = new ServiceInfo(clusterId, serviceId, updatedChildList);
            updated((T) updated);
        }
    }

    @Override
    public void nodeDataChanged(byte[] updatedData) {
        if (nodeId != null) {
            Map<String, String> attributeMap = nodeAttributeSerializer.deserialize(updatedData);
            NodeInfo updated = new NodeInfo(clusterId, serviceId, nodeId, attributeMap);
            updated((T) updated);
        }
    }

    @Override
    public void nodeDeleted() {
        if (nodeId != null) {
            updated(null);
        }
    }

    @Override
    public void nodeCreated(byte[] data) {
        if (nodeId != null) {
            Map<String, String> attributeMap = nodeAttributeSerializer.deserialize(data);
            NodeInfo updated = new NodeInfo(clusterId, serviceId, nodeId, attributeMap);
            updated((T) updated);
        }
    }

}
