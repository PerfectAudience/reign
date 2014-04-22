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
public class ParsedRequestMessage implements RequestMessage {

    private static final Logger logger = LoggerFactory.getLogger(ParsedRequestMessage.class);

    private final RequestMessage mesg;

    private String meta;
    private String resource;

    public ParsedRequestMessage(RequestMessage mesg) {
        this.mesg = mesg;

        // parse and populate useful fields
        String body = (String) mesg.getBody();
        int firstHashIndex = body.indexOf("#");
        int firstNewlineIndex = body.indexOf("\n");
        if (firstHashIndex < 0) {
            if (firstNewlineIndex < 0) {
                resource = body.trim();
            } else {
                resource = body.substring(0, firstNewlineIndex).trim();
            }
            meta = null;
        } else {
            resource = body.substring(0, firstHashIndex).trim();
            if (firstNewlineIndex < 0) {
                meta = body.substring(firstHashIndex + 1).trim();
            } else {
                meta = body.substring(firstHashIndex + 1, firstNewlineIndex).trim();
            }
        }

    }

    public String getMeta() {
        return meta;
    }

    public void setMeta(String meta) {
        this.meta = meta;
    }

    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    @Override
    public NodeId getSenderId() {
        return mesg.getSenderId();
    }

    @Override
    public RequestMessage setSenderId(NodeId senderId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Integer getId() {
        return mesg.getId();
    }

    @Override
    public Message setId(Integer id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getTargetService() {
        return mesg.getTargetService();
    }

    @Override
    public RequestMessage setTargetService(String targetService) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getBody() {
        return mesg.getBody();
    }

    @Override
    public Message setBody(Object body) {
        throw new UnsupportedOperationException();
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
