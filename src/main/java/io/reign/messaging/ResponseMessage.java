package io.reign.messaging;

/**
 * 
 * @author ypai
 * 
 */
public interface ResponseMessage extends Message {
    public ResponseStatus getStatus();

    public ResponseMessage setStatus(ResponseStatus status);
}
