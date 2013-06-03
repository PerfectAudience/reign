package org.kompany.sovereign.messaging;

import java.util.Map;

import org.kompany.sovereign.AbstractService;
import org.kompany.sovereign.PathScheme;

/**
 * Wrapper service to make messaging capabilities available to other services via ServiceDirectory.
 * 
 * @author ypai
 * 
 */
public class MessagingService extends AbstractService {

    private MessagingProvider messagingProvider;

    public void sendMessage(String cluster, String serviceId, String nodeId, RequestMessage message) {
        Map<String, String> canonicalIdMap = getPathScheme().parseCanonicalId(nodeId);

        // prefer ip, then use hostname if not available
        String hostOrIpAddress = canonicalIdMap.get(PathScheme.CANONICAL_ID_IP);
        if (hostOrIpAddress == null) {
            hostOrIpAddress = canonicalIdMap.get(PathScheme.CANONICAL_ID_HOST);
        }

        // get port
        int port = Integer.parseInt(canonicalIdMap.get(PathScheme.CANONICAL_ID_PORT));

        this.messagingProvider.sendMessage(hostOrIpAddress, port, message);

    }

    public MessagingProvider getMessagingProvider() {
        return messagingProvider;
    }

    public void setMessagingProvider(MessagingProvider messagingProvider) {
        this.messagingProvider = messagingProvider;
    }

    @Override
    public void init() {
        this.messagingProvider.start();
    }

    @Override
    public void destroy() {
        this.messagingProvider.stop();
    }

}
