package io.reign;

import io.reign.util.IdUtil;

/**
 * 
 * @author ypai
 * 
 */
public class DefaultCanonicalIdMaker implements CanonicalIdMaker {

    private String processId;
    private String host;
    private String ipAddress;

    private final Integer messagingPort;

    public DefaultCanonicalIdMaker() {
        this(null);
    }

    public DefaultCanonicalIdMaker(Integer messagingPort) {
        this.messagingPort = messagingPort;

        // get pid
        processId = IdUtil.getProcessId();

        // try to get hostname and ip address
        host = IdUtil.getHostname();
        ipAddress = IdUtil.getIpAddress();

        // fill in unknown values
        if (processId == null) {
            processId = "";
        }
        if (host == null) {
            host = "";
        }
        if (ipAddress == null) {
            ipAddress = "";
        }
    }

    @Override
    public CanonicalId id() {

        CanonicalId id = new DefaultCanonicalId();
        id.setProcessId(processId);
        id.setHost(host);
        id.setIpAddress(ipAddress);
        id.setMessagingPort(messagingPort);

        return id;

    }

}
