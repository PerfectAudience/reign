package io.reign.mesg;

import io.reign.NodeId;
import io.reign.util.JacksonUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class SimpleRequestMessage extends AbstractMessage implements RequestMessage {

    private static final Logger logger = LoggerFactory.getLogger(SimpleRequestMessage.class);

    private String targetService;

    private NodeId senderId;

    public SimpleRequestMessage() {

    }

    public SimpleRequestMessage(String targetService, Object body) {
        setTargetService(targetService);
        setBody(body);
    }

    @Override
    public NodeId getSenderId() {
        return senderId;
    }

    @Override
    public RequestMessage setSenderId(NodeId senderId) {
        this.senderId = senderId;
        return this;
    }

    @Override
    public String getTargetService() {
        return targetService;
    }

    @Override
    public RequestMessage setTargetService(String targetService) {
        this.targetService = targetService;
        return this;
    }

    @Override
    public String toString() {
        try {
            return JacksonUtil.getObjectMapper().writeValueAsString(this);
        } catch (Exception e) {
            logger.error("" + e, e);
            return super.toString();
        }
    }

}
