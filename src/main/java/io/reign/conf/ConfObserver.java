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

package io.reign.conf;

import io.reign.AbstractNodeObserver;
import io.reign.DataSerializer;

import java.util.Map;

/**
 * 
 * @author ypai
 * 
 */
public abstract class ConfObserver<T> extends AbstractNodeObserver {

    private String clusterId = null;
    private String serviceId = null;
    private String nodeId = null;

    private Map<String, DataSerializer> dataSerializerMap = null;

    public abstract void updated(T updated, T existing);

    public Map<String, DataSerializer> getDataSerializerMap() {
        return dataSerializerMap;
    }

    public void setDataSerializerMap(Map<String, DataSerializer> dataSerializerMap) {
        this.dataSerializerMap = dataSerializerMap;
    }

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
    public void nodeDataChanged(byte[] updatedData) {
        updated(toConf(updatedData), toConf(getPreviousData()));
    }

    @Override
    public void nodeDeleted() {
        updated(null, toConf(getPreviousData()));
    }

    @Override
    public void nodeCreated(byte[] data) {
        updated(toConf(data), toConf(getPreviousData()));
    }

    T toConf(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        DataSerializer<T> transcoder = ConfService.getDataSerializer(getPath(), dataSerializerMap);
        return transcoder.deserialize(data);
    }

}
