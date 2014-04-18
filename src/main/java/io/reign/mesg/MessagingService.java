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

package io.reign.mesg;

import io.reign.NodeId;
import io.reign.Service;

import java.util.Map;

/**
 * 
 * @author ypai
 * 
 */
public interface MessagingService extends Service {

    public ResponseMessage sendMessage(String clusterId, String serviceId, NodeId nodeId, RequestMessage requestMessage);

    public Map<String, ResponseMessage> sendMessage(String clusterId, String serviceId, RequestMessage requestMessage);

    public void sendMessageAsync(String clusterId, String serviceId, NodeId nodeId, RequestMessage requestMessage,
            MessagingCallback callback);

    public void sendMessageAsync(String clusterId, String serviceId, RequestMessage requestMessage,
            MessagingCallback callback);

    public void sendMessageFF(String clusterId, String serviceId, NodeId nodeId, EventMessage eventMessage);

    public void sendMessageFF(String clusterId, String serviceId, NodeId nodeId, RequestMessage requestMessage);

    public void sendMessageFF(String clusterId, String serviceId, RequestMessage requestMessage);

    public Integer getPort();

    /**
     * 
     * @param port
     *            cannot be null
     */
    public void setPort(Integer port);
}
