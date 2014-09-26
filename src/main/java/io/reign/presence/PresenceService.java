/*
 * Copyright 2013 Yen Pai ypai@reign.io
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.reign.presence;

import io.reign.DataSerializer;
import io.reign.NodeInfo;
import io.reign.Service;
import io.reign.ServiceNodeInfo;

import java.util.List;
import java.util.Map;

/**
 * Service discovery service.
 * 
 * @author ypai
 * 
 */
public interface PresenceService extends Service {

    public boolean isMemberOf(String clusterId);

    public boolean isMemberOf(String clusterId, String serviceId);

    public List<String> getClusters();

    public List<String> getServices(String clusterId);

    public void observe(String clusterId, PresenceObserver<List<String>> observer);

    public void observe(String clusterId, String serviceId, PresenceObserver<ServiceInfo> observer);

    public void observe(String clusterId, String serviceId, String nodeId, PresenceObserver<? extends NodeInfo> observer);

    public ServiceInfo waitUntilAvailable(String clusterId, String serviceId, long timeoutMillis);

    public ServiceInfo getServiceInfo(String clusterId, String serviceId);

    public ServiceInfo getServiceInfo(String clusterId, String serviceId, PresenceObserver<ServiceInfo> observer);

    public ServiceNodeInfo waitUntilAvailable(String clusterId, String serviceId, String nodeId, long timeoutMillis);

    public ServiceNodeInfo getNodeInfo(String clusterId, String serviceId, String nodeId);

    public ServiceNodeInfo getNodeInfo(String clusterId, String serviceId, String nodeId,
            PresenceObserver<ServiceNodeInfo> observer);

    public void announce(String clusterId, String serviceId);

    public void announce(String clusterId, String serviceId, boolean visible);

    public void announce(String clusterId, String serviceId, boolean visible, Map<String, String> attributeMap);

    /**
     * Used to track connected clients.
     */
    public void announce(String clusterId, String serviceId, String nodeId, boolean visible);

    public void hide(String clusterId, String serviceId);

    public void show(String clusterId, String serviceId);

    /**
     * Used to flag that a service node is dead, the presence node should be removed, and should not be checked again.
     * 
     * Used internally to remove connected clients once ping(s) fail.
     * 
     * @param clusterId
     * @param serviceId
     * @param nodeId
     */
    public void dead(String clusterId, String serviceId, String nodeId);

    public void dead(String clusterId, String serviceId);

    public DataSerializer<Map<String, String>> getNodeAttributeSerializer();

    public void setNodeAttributeSerializer(DataSerializer<Map<String, String>> nodeAttributeSerializer);

    public long getHeartbeatIntervalMillis();

    public void setHeartbeatIntervalMillis(int heartbeatIntervalMillis);

    public int getZombieCheckIntervalMillis();

    public void setZombieCheckIntervalMillis(int zombieCheckIntervalMillis);

}
