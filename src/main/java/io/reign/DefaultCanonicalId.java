package io.reign;

/**
 * 
 * @author ypai
 * 
 */
public class DefaultCanonicalId implements CanonicalId {

    /**
     * process ID
     */
    private String processId;

    /**
     * IP address of node
     */
    private String ipAddress;

    /**
     * host name of node
     */
    private String host;

    /**
     * the port for an available service (short-cut to specifying it as an node attribute -- fewer ZK operations if this
     * is all that is needed to interact with a service once discovered)
     */
    private Integer port;

    /** the messaging port for the framework */
    private Integer messagingPort;

    public static DefaultCanonicalId id() {
        return new DefaultCanonicalId();
    }

    @Override
    public DefaultCanonicalId port(Integer port) {
        setPort(port);
        return this;
    }

    @Override
    public String getProcessId() {
        return processId;
    }

    @Override
    public void setProcessId(String processId) {
        this.processId = processId;
    }

    @Override
    public String getIpAddress() {
        return ipAddress;
    }

    @Override
    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public void setHost(String host) {
        this.host = host;
    }

    @Override
    public Integer getPort() {
        return port;
    }

    @Override
    public void setPort(Integer port) {
        this.port = port;
    }

    @Override
    public Integer getMessagingPort() {
        return messagingPort;
    }

    @Override
    public void setMessagingPort(Integer messagingPort) {
        this.messagingPort = messagingPort;
    }

}
