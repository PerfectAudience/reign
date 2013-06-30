package io.reign;

import io.reign.util.IdUtil;

/**
 * 
 * @author ypai
 * 
 */
public class DefaultCanonicalIdMaker implements CanonicalIdMaker {

    private Integer messagingPort;

    @Override
    public CanonicalId id() {
        // get pid
        String pid = IdUtil.getProcessId();

        // try to get hostname and ip address
        String hostname = IdUtil.getHostname();
        String ipAddress = IdUtil.getIpAddress();

        // fill in unknown values
        if (pid == null) {
            pid = "";
        }
        if (hostname == null) {
            hostname = "";
        }
        if (ipAddress == null) {
            ipAddress = "";
        }

        CanonicalId id = new DefaultCanonicalId();
        id.setProcessId(pid);
        id.setHost(hostname);
        id.setIpAddress(ipAddress);
        id.setMessagingPort(messagingPort);

        return id;

    }

    public Integer getMessagingPort() {
        return messagingPort;
    }

    public void setMessagingPort(Integer messagingPort) {
        this.messagingPort = messagingPort;
    }

}
