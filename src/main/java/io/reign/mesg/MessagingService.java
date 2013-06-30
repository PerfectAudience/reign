package io.reign.mesg;

import io.reign.Service;

import java.util.Map;

/**
 * 
 * @author ypai
 * 
 */
public interface MessagingService extends Service {

    /**
     * 
     * @param clusterId
     * @param serviceId
     * @param canonicalIdString
     * @param requestMessage
     * @return
     */
    public ResponseMessage sendMessage(String clusterId, String serviceId, String canonicalIdString,
            RequestMessage requestMessage);

    /**
     * 
     * @param clusterId
     * @param serviceId
     * @param requestMessage
     * @return
     */
    public Map<String, ResponseMessage> sendMessage(String clusterId, String serviceId, RequestMessage requestMessage);

    public Integer getPort();

    /**
     * 
     * @param port
     *            cannot be null
     */
    public void setPort(Integer port);
}
