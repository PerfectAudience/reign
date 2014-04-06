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

import io.reign.AbstractService;
import io.reign.NodeId;
import io.reign.Reign;
import io.reign.ZkNodeId;
import io.reign.mesg.websocket.WebSocketMessagingProvider;
import io.reign.presence.PresenceService;
import io.reign.presence.ServiceInfo;
import io.reign.util.JacksonUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Makes messaging capabilities available to other services via context.
 * 
 * @author ypai
 * 
 */
public class DefaultMessagingService extends AbstractService implements MessagingService {

    private static final Logger logger = LoggerFactory.getLogger(DefaultMessagingService.class);

    private static final MessagingProviderCallback NULL_MESSAGING_PROVIDER_CALLBACK = new NullMessagingProviderCallback();

    private Integer port = Reign.DEFAULT_MESSAGING_PORT;

    private MessagingProvider messagingProvider = new WebSocketMessagingProvider();

    private MessageProtocol messageProtocol = new DefaultMessageProtocol();

    private static ObjectMapper OBJECT_MAPPER = JacksonUtil.getObjectMapper();

    /**
     * Send message to a single node.
     * 
     * @param clusterId
     * @param serviceId
     * @param canonicalId
     * @param requestMessage
     * @return
     */
    @Override
    public ResponseMessage sendMessage(final String clusterId, final String serviceId, final NodeId nodeId,
            final RequestMessage requestMessage) {

        // prefer ip, then use hostname if not available
        String hostOrIpAddress = nodeId.getIpAddress();
        if (hostOrIpAddress == null) {
            hostOrIpAddress = nodeId.getHost();
        }

        // get port
        Integer port = nodeId.getMessagingPort();

        if (hostOrIpAddress == null || port == null) {
            throw new IllegalStateException("Host or port is not available:  host=" + hostOrIpAddress + "; port="
                    + port);
        }

        // send message and wait for response
        final AtomicReference<ResponseMessage> responseValue = new AtomicReference<ResponseMessage>(null);
        MessagingCallback messagingCallback = new MessagingCallback() {
            @Override
            public void response(String clusterId, String serviceId, NodeId nodeId, ResponseMessage responseMessage) {
                responseValue.set(responseMessage);
                synchronized (this) {
                    this.notifyAll();
                }
            }
        };
        sendMessageAsync(clusterId, serviceId, nodeId, requestMessage, messagingCallback);
        synchronized (messagingCallback) {
            try {
                messagingCallback.wait();
            } catch (InterruptedException e) {
                logger.warn("Interrupted while waiting for response:  " + e, e);
            }
        }
        return responseValue.get();
    }

    /**
     * Send a message to all nodes belonging to a service.
     * 
     * @param clusterId
     * @param serviceId
     * @param requestMessage
     * @return
     */
    @Override
    public Map<String, ResponseMessage> sendMessage(String clusterId, String serviceId, RequestMessage requestMessage) {
        PresenceService presenceService = getContext().getService("presence");
        ServiceInfo serviceInfo = presenceService.getServiceInfo(clusterId, serviceId);
        if (serviceInfo == null) {
            return Collections.EMPTY_MAP;
        }
        Map<String, ResponseMessage> responseMap = new HashMap<String, ResponseMessage>(serviceInfo.getNodeIdList()
                .size());
        for (String nodeIdString : serviceInfo.getNodeIdList()) {
            if (logger.isTraceEnabled()) {
                logger.trace("Sending message:  clusterId={}; serviceId={}; nodeId={}; requestMessage={}",
                        new Object[] { clusterId, serviceId, nodeIdString, requestMessage });
            }
            ResponseMessage responseMessage = sendMessage(clusterId, serviceId,
                    getContext().getNodeIdFromZk(new ZkNodeId(nodeIdString, null)), requestMessage);
            responseMap.put(getPathScheme().joinTokens(clusterId, serviceId, nodeIdString), responseMessage);
        }

        return responseMap;
    }

    void sendMessageAsync(final String clusterId, final String serviceId, final NodeId nodeId,
            final RequestMessage requestMessage, final MessagingCallback callback) {

        // prefer ip, then use hostname if not available
        String hostOrIpAddress = nodeId.getIpAddress();
        if (hostOrIpAddress == null) {
            hostOrIpAddress = nodeId.getHost();
        }

        // get port
        Integer port = nodeId.getMessagingPort();

        if (hostOrIpAddress == null || port == null) {
            throw new IllegalStateException("Host or port is not available:  host=" + hostOrIpAddress + "; port="
                    + port);
        }

        if (requestMessage.getBody() instanceof String) {
            MessagingProviderCallback messagingProviderCallback = new MessagingProviderCallback() {
                @Override
                public void response(String response) {
                    ResponseMessage responseMessage = messageProtocol.fromTextResponse(response);
                    // responseMessage.setId(requestMessage.getId());
                    callback.response(clusterId, serviceId, nodeId, responseMessage);
                }

                @Override
                public void response(byte[] bytes) {
                }

                @Override
                public void error(Object object) {
                    callback.response(clusterId, serviceId, nodeId, new SimpleResponseMessage(
                            ResponseStatus.ERROR_UNEXPECTED, requestMessage.getId()));
                }
            };
            this.messagingProvider.sendMessage(hostOrIpAddress, port, messageProtocol.toTextRequest(requestMessage),
                    messagingProviderCallback);

        } else {
            MessagingProviderCallback messagingProviderCallback = new MessagingProviderCallback() {
                @Override
                public void response(String response) {
                }

                @Override
                public void response(byte[] bytes) {
                    ResponseMessage responseMessage = messageProtocol.fromBinaryResponse(bytes);
                    // responseMessage.setId(requestMessage.getId());
                    callback.response(clusterId, serviceId, nodeId, responseMessage);
                }

                @Override
                public void error(Object object) {
                    callback.response(clusterId, serviceId, nodeId, new SimpleResponseMessage(
                            ResponseStatus.ERROR_UNEXPECTED, requestMessage.getId()));
                }
            };
            this.messagingProvider.sendMessage(hostOrIpAddress, port, messageProtocol.toBinaryRequest(requestMessage),
                    messagingProviderCallback);

        }
    }

    @Override
    public void sendMessageAsync(String clusterId, String serviceId, RequestMessage requestMessage,
            MessagingCallback callback) {
        PresenceService presenceService = getContext().getService("presence");
        ServiceInfo serviceInfo = presenceService.getServiceInfo(clusterId, serviceId);
        if (serviceInfo == null) {
            return;
        }

        for (String nodeIdString : serviceInfo.getNodeIdList()) {
            if (logger.isTraceEnabled()) {
                logger.trace("Sending message:  clusterId={}; serviceId={}; nodeId={}; requestMessage={}",
                        new Object[] { clusterId, serviceId, nodeIdString, requestMessage });
            }
            sendMessageAsync(clusterId, serviceId, getContext().getNodeIdFromZk(new ZkNodeId(nodeIdString, null)),
                    requestMessage, callback);

        }
    }

    void sendMessageFireAndForget(String clusterId, String serviceId, NodeId nodeId, RequestMessage requestMessage) {

        // prefer ip, then use hostname if not available
        String hostOrIpAddress = nodeId.getIpAddress();
        if (hostOrIpAddress == null) {
            hostOrIpAddress = nodeId.getHost();
        }

        // get port
        Integer port = nodeId.getMessagingPort();

        if (hostOrIpAddress == null || port == null) {
            throw new IllegalStateException("Host or port is not available:  host=" + hostOrIpAddress + "; port="
                    + port);
        }

        if (requestMessage.getBody() instanceof String) {
            this.messagingProvider.sendMessage(hostOrIpAddress, port, messageProtocol.toTextRequest(requestMessage),
                    NULL_MESSAGING_PROVIDER_CALLBACK);

        } else {
            this.messagingProvider.sendMessage(hostOrIpAddress, port, messageProtocol.toBinaryRequest(requestMessage),
                    NULL_MESSAGING_PROVIDER_CALLBACK);

        }

    }

    @Override
    public void sendMessageFireAndForget(String clusterId, String serviceId, RequestMessage requestMessage) {
        PresenceService presenceService = getContext().getService("presence");
        ServiceInfo serviceInfo = presenceService.getServiceInfo(clusterId, serviceId);
        if (serviceInfo == null) {
            return;
        }

        for (String nodeIdString : serviceInfo.getNodeIdList()) {
            if (logger.isTraceEnabled()) {
                logger.trace("Sending message:  clusterId={}; serviceId={}; nodeId={}; requestMessage={}",
                        new Object[] { clusterId, serviceId, nodeIdString, requestMessage });
            }
            sendMessageFireAndForget(clusterId, serviceId,
                    getContext().getNodeIdFromZk(new ZkNodeId(nodeIdString, null)), requestMessage);

        }
    }

    public MessagingProvider getMessagingProvider() {
        return messagingProvider;
    }

    public void setMessagingProvider(MessagingProvider messagingProvider) {
        this.messagingProvider = messagingProvider;
    }

    public MessageProtocol getMessageProtocol() {
        return messageProtocol;
    }

    public void setMessageProtocol(MessageProtocol messageProtocol) {
        this.messageProtocol = messageProtocol;
    }

    @Override
    public Integer getPort() {
        return port;
    }

    @Override
    public void setPort(Integer port) {
        if (port == null) {
            throw new IllegalArgumentException("Invalid argument:  'port' cannot be null!");
        }
        this.port = port;
    }

    @Override
    public void init() {
        logger.info("Starting messaging service:  port={}", port);

        this.messagingProvider.setMessageProtocol(messageProtocol);
        this.messagingProvider.setServiceDirectory(getContext());
        this.messagingProvider.setPort(port);
        this.messagingProvider.init();
    }

    @Override
    public void destroy() {
        logger.info("Shutting down messaging service:  port={}", port);

        this.messagingProvider.destroy();
    }

    @Override
    public ResponseMessage handleMessage(RequestMessage requestMessage) {
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("Received message:  request='{}:{}'", requestMessage.getTargetService(),
                        requestMessage.getBody());
            }

            /** preprocess request **/
            String requestBody = (String) requestMessage.getBody();
            int newlineIndex = requestBody.indexOf("\n");
            String resourceLine = requestBody;
            String mesgBody = null;
            if (newlineIndex != -1) {
                resourceLine = requestBody.substring(0, newlineIndex);
                mesgBody = requestBody.substring(newlineIndex + 1);
            }

            RequestMessage messageToSend = OBJECT_MAPPER.readValue(mesgBody, new TypeReference<SimpleRequestMessage>() {
            });

            // get meta
            String meta = null;
            String resource = null;
            int hashLastIndex = resourceLine.lastIndexOf("#");
            if (hashLastIndex != -1) {
                meta = resourceLine.substring(hashLastIndex + 1).trim();
                resource = resourceLine.substring(0, hashLastIndex);
            } else {
                resource = resourceLine;
            }

            // get resource; strip beginning and ending slashes "/"
            if (resource.startsWith("/")) {
                resource = resource.substring(1);
            }
            if (resource.endsWith("/")) {
                resource = resource.substring(0, resource.length() - 1);
            }

            // get target
            String[] tokens = getPathScheme().tokenizePath(resource);
            String clusterId = tokens[0];
            String serviceId = tokens[1];
            String nodeIdString = null;
            if (tokens.length > 2) {
                nodeIdString = tokens[2];
            }

            logger.debug("clusterId={}; serviceId={}; nodeId={}; meta={}; mesgBody={}", new Object[] { clusterId,
                    serviceId, nodeIdString, meta, mesgBody });

            /** take appropriate action **/
            if (nodeIdString == null) {
                if ("ff".equals(meta)) {
                    this.sendMessageFireAndForget(clusterId, serviceId, messageToSend);
                } else {
                    return new SimpleResponseMessage(ResponseStatus.ERROR_UNEXPECTED, "Unrecognized meta:  '" + meta
                            + "'");
                }
            } else {
                if ("ff".equals(meta)) {
                    this.sendMessageFireAndForget(clusterId, serviceId,
                            getContext().getNodeIdFromZk(new ZkNodeId(nodeIdString, null)), messageToSend);
                } else {
                    return new SimpleResponseMessage(ResponseStatus.ERROR_UNEXPECTED, "Unrecognized meta:  '" + meta
                            + "'");
                }
            }

            return new SimpleResponseMessage(ResponseStatus.OK, requestMessage.getId());

        } catch (Exception e) {
            logger.error("" + e, e);
            return new SimpleResponseMessage(ResponseStatus.ERROR_UNEXPECTED, requestMessage.getId());
        }

    }
}
