package io.reign.mesg;

import io.reign.util.JacksonUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class SimpleResponseMessage extends AbstractMessage implements ResponseMessage {

    private static final Logger logger = LoggerFactory.getLogger(SimpleResponseMessage.class);

    public static final ResponseMessage DEFAULT_ERROR_RESPONSE = new SimpleResponseMessage(
            ResponseStatus.ERROR_UNEXPECTED);

    public static final ResponseMessage DEFAULT_OK_RESPONSE = new SimpleResponseMessage(ResponseStatus.OK);

    private ResponseStatus status = ResponseStatus.OK;

    private String comment = null;

    public SimpleResponseMessage() {
    }

    public SimpleResponseMessage(ResponseStatus status) {
        setStatus(status);
    }

    public SimpleResponseMessage(ResponseStatus status, Object body) {
        setStatus(status);
        setBody(body);
    }

    @Override
    public ResponseStatus getStatus() {
        return status;
    }

    @Override
    public String getComment() {
        return comment;
    }

    @Override
    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public ResponseMessage setStatus(ResponseStatus status) {
        this.status = status;
        return this;
    }

    @Override
    public ResponseMessage setStatus(ResponseStatus status, String comment) {
        this.status = status;
        this.comment = comment;
        return this;
    }

    @Override
    public String toString() {
        try {
            return JacksonUtil.getObjectMapperInstance().writeValueAsString(this);
        } catch (Exception e) {
            logger.error("" + e, e);
            return super.toString();
        }
    }

}
