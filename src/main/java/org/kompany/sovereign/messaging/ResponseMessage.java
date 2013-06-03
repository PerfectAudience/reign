package org.kompany.sovereign.messaging;

/**
 * 
 * @author ypai
 * 
 */
public interface ResponseMessage extends Message {
    public ResponseStatus getStatus();

    public void setStatus(ResponseStatus status);
}
