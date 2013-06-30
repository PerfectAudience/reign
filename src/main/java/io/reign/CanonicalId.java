package io.reign;

/**
 * 
 * @author ypai
 * 
 */
public interface CanonicalId {

    public CanonicalId port(Integer port);

    public String getProcessId();

    public void setProcessId(String processId);

    public String getIpAddress();

    public void setIpAddress(String ipAddress);

    public String getHost();

    public void setHost(String host);

    public Integer getPort();

    public void setPort(Integer port);

    public Integer getMessagingPort();

    public void setMessagingPort(Integer messagingPort);

}
