package io.reign.messaging;

/**
 * 
 * @author ypai
 * 
 */
public interface RequestMessage extends Message {

    public String getTargetService();

    public void setTargetService(String targetService);
}
