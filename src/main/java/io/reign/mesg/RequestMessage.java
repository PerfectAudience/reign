package io.reign.mesg;

import io.reign.NodeId;

/**
 * 
 * @author ypai
 * 
 */
public interface RequestMessage extends Message {

    public String getTargetService();

    public RequestMessage setTargetService(String targetService);

    /**
     * @return the sender of the request
     */
    public NodeId getSenderId();

    public RequestMessage setSenderId(NodeId senderId);
}
