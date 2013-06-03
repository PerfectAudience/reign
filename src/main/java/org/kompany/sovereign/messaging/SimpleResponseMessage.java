package org.kompany.sovereign.messaging;

/**
 * 
 * @author ypai
 * 
 */
public class SimpleResponseMessage extends AbstractMessage implements ResponseMessage {

    private ResponseStatus status = ResponseStatus.OK;

    public SimpleResponseMessage() {
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
    public void setStatus(ResponseStatus status) {
        this.status = status;
    }

}
