package io.reign.mesg;

/**
 * 
 * @author ypai
 * 
 */
public interface ResponseMessage extends Message {
    public ResponseStatus getStatus();

    public ResponseMessage setStatus(ResponseStatus status);
}
