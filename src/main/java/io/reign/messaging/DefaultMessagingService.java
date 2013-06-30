package io.reign.messaging;

import io.reign.AbstractService;
import io.reign.CanonicalId;
import io.reign.messaging.websocket.WebSocketMessagingProvider;
import io.reign.presence.PresenceService;
import io.reign.presence.ServiceInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper service to make messaging capabilities available to other services via context.
 * 
 * @author ypai
 * 
 */
public class DefaultMessagingService extends AbstractService implements MessagingService {

    private static final Logger logger = LoggerFactory.getLogger(DefaultMessagingService.class);

    private Integer port = 33033;

    private MessagingProvider messagingProvider = new WebSocketMessagingProvider();

    private MessageProtocol messageProtocol = new DefaultMessageProtocol();

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
    public ResponseMessage sendMessage(String clusterId, String serviceId, String canonicalIdString,
            RequestMessage requestMessage) {
        CanonicalId canonicalId = getPathScheme().parseCanonicalId(canonicalIdString);

        // prefer ip, then use hostname if not available
        String hostOrIpAddress = canonicalId.getIpAddress();
        if (hostOrIpAddress == null) {
            hostOrIpAddress = canonicalId.getHost();
        }

        // get port
        Integer port = canonicalId.getMessagingPort();

        if (hostOrIpAddress == null || port == null) {
            throw new IllegalStateException("Host or port is not available:  host=" + hostOrIpAddress + "; port="
                    + port);
        }

        if (requestMessage.getBody() instanceof String) {
            String textResponse = this.messagingProvider.sendMessage(hostOrIpAddress, port, messageProtocol
                    .toTextRequest(requestMessage));
            return this.messageProtocol.fromTextResponse(textResponse);
        } else {
            byte[] binaryResponse = this.messagingProvider.sendMessage(hostOrIpAddress, port, messageProtocol
                    .toBinaryRequest(requestMessage));
            return this.messageProtocol.fromBinaryResponse(binaryResponse);
        }

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
        ServiceInfo serviceInfo = presenceService.lookupServiceInfo(clusterId, serviceId);
        if (serviceInfo == null) {
            return Collections.EMPTY_MAP;
        }
        Map<String, ResponseMessage> responseMap = new HashMap<String, ResponseMessage>(serviceInfo.getNodeIdList()
                .size());
        for (String nodeId : serviceInfo.getNodeIdList()) {
            if (logger.isTraceEnabled()) {
                logger.trace("Sending message:  clusterId={}; serviceId={}; nodeId={}; requestMessage={}",
                        new Object[] { clusterId, serviceId, nodeId, requestMessage });
            }
            ResponseMessage responseMessage = sendMessage(clusterId, serviceId, nodeId, requestMessage);
            responseMap.put(getPathScheme().buildRelativePath(clusterId, serviceId, nodeId), responseMessage);
        }

        return responseMap;
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
        this.messagingProvider.start();
    }

    @Override
    public void destroy() {
        logger.info("Shutting down messaging service:  port={}", port);

        this.messagingProvider.stop();
    }

}
