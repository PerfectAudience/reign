package io.reign.mesg;

import io.reign.AbstractService;
import io.reign.CanonicalId;
import io.reign.Reign;
import io.reign.mesg.websocket.WebSocketMessagingProvider;
import io.reign.presence.PresenceService;
import io.reign.presence.ServiceInfo;
import io.reign.util.JacksonUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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

    private Integer port = Reign.DEFAULT_MESSAGING_PORT;

    private MessagingProvider messagingProvider = new WebSocketMessagingProvider();

    private MessageProtocol messageProtocol = new DefaultMessageProtocol();

    private static ObjectMapper OBJECT_MAPPER = JacksonUtil.getObjectMapperInstance();

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
        CanonicalId canonicalId = getContext().getCanonicalIdProvider().from(canonicalIdString, null);

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
            String textResponse = this.messagingProvider.sendMessage(hostOrIpAddress, port,
                    messageProtocol.toTextRequest(requestMessage));
            return this.messageProtocol.fromTextResponse(textResponse);
        } else {
            byte[] binaryResponse = this.messagingProvider.sendMessage(hostOrIpAddress, port,
                    messageProtocol.toBinaryRequest(requestMessage));
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
            responseMap.put(getPathScheme().joinTokens(clusterId, serviceId, nodeId), responseMessage);
        }

        return responseMap;
    }

    public void sendMessageForget(String clusterId, String serviceId, String canonicalIdString,
            RequestMessage requestMessage) {
        CanonicalId canonicalId = getContext().getCanonicalIdProvider().from(canonicalIdString, null);

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
            this.messagingProvider.sendMessageForget(hostOrIpAddress, port,
                    messageProtocol.toTextRequest(requestMessage));

        } else {
            this.messagingProvider.sendMessageForget(hostOrIpAddress, port,
                    messageProtocol.toBinaryRequest(requestMessage));

        }

    }

    public void sendMessageForget(String clusterId, String serviceId, RequestMessage requestMessage) {
        PresenceService presenceService = getContext().getService("presence");
        ServiceInfo serviceInfo = presenceService.lookupServiceInfo(clusterId, serviceId);
        if (serviceInfo == null) {
            return;
        }

        for (String nodeId : serviceInfo.getNodeIdList()) {
            if (logger.isTraceEnabled()) {
                logger.trace("Sending message:  clusterId={}; serviceId={}; nodeId={}; requestMessage={}",
                        new Object[] { clusterId, serviceId, nodeId, requestMessage });
            }
            sendMessageForget(clusterId, serviceId, nodeId, requestMessage);

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
                meta = resourceLine.substring(hashLastIndex + 1);
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
            String nodeId = null;
            if (tokens.length > 2) {
                nodeId = tokens[2];
            }

            logger.debug("clusterId={}; serviceId={}; nodeId={}; meta={}; mesgBody={}", new Object[] { clusterId,
                    serviceId, nodeId, meta, mesgBody });

            /** take appropriate action **/
            if (nodeId == null) {
                if ("forget".equals(meta)) {
                    this.sendMessageForget(clusterId, serviceId, messageToSend);
                } else {
                    return new SimpleResponseMessage(ResponseStatus.ERROR_UNEXPECTED, "Unrecognized meta:  " + meta);
                }
            } else {
                if ("forget".equals(meta)) {
                    this.sendMessageForget(clusterId, serviceId, nodeId, messageToSend);
                } else {
                    return new SimpleResponseMessage(ResponseStatus.ERROR_UNEXPECTED, "Unrecognized meta:  " + meta);
                }
            }

            return SimpleResponseMessage.DEFAULT_OK_RESPONSE;

        } catch (Exception e) {
            logger.error("" + e, e);
            return SimpleResponseMessage.DEFAULT_ERROR_RESPONSE;
        }

    }
}
