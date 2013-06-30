package io.reign.messaging;

import io.reign.Service;

import java.util.Map;

/**
 * 
 * @author ypai
 * 
 */
public interface MessagingService extends Service {

    public ResponseMessage sendMessage(String clusterId, String serviceId, String canonicalIdString,
            RequestMessage requestMessage);

    public Map<String, ResponseMessage> sendMessage(String clusterId, String serviceId, RequestMessage requestMessage);

    public Integer getPort();

    public void setPort(Integer port);
}
