package org.kompany.sovereign.messaging;

/**
 * 
 * @author ypai
 * 
 */
public interface RequestMessage extends Message {

    public String getTargetService();

    public void setTargetService(String targetService);
}
