package io.reign.messaging;

import io.reign.AbstractService;
import io.reign.PathScheme;
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
public class MessagingService extends AbstractService {

    private static final Logger logger = LoggerFactory.getLogger(MessagingService.class);

    private int port = 33033;

    private MessagingProvider messagingProvider = new WebSocketMessagingProvider();

    private MessageProtocol messageProtocol = new DefaultMessageProtocol();

    /**
     * Send message to a single node.
     * 
     * @param clusterId
     * @param serviceId
     * @param nodeId
     * @param requestMessage
     * @return
     */
    public ResponseMessage sendMessage(String clusterId, String serviceId, String nodeId, RequestMessage requestMessage) {
        Map<String, String> canonicalIdMap = getPathScheme().parseCanonicalId(nodeId);

        // prefer ip, then use hostname if not available
        String hostOrIpAddress = canonicalIdMap.get(PathScheme.CANONICAL_ID_IP);
        if (hostOrIpAddress == null) {
            hostOrIpAddress = canonicalIdMap.get(PathScheme.CANONICAL_ID_HOST);
        }

        // get port
        String portString = canonicalIdMap.get(PathScheme.CANONICAL_ID_MESSAGING_PORT);

        if (hostOrIpAddress == null || portString == null) {
            return null;
        }

        int port = -1;
        try {
            port = Integer.parseInt(portString);
        } catch (Exception e1) {
            logger.error("" + e1, e1);
            return null;
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

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public void init() {
        this.messagingProvider.setMessageProtocol(messageProtocol);
        this.messagingProvider.setServiceDirectory(getContext());
        this.messagingProvider.setPort(port);
        this.messagingProvider.start();
    }

    @Override
    public void destroy() {
        this.messagingProvider.stop();
    }

}
