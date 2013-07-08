package io.reign.mesg;

/**
 * 
 * @author ypai
 * 
 */
public interface ResponseMessage extends Message {
    public ResponseStatus getStatus();

    public String getComment();

    public void setComment(String comment);

    public ResponseMessage setStatus(ResponseStatus status);

    public ResponseMessage setStatus(ResponseStatus status, String comment);
}
