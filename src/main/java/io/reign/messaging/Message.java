package io.reign.messaging;

/**
 * 
 * @author ypai
 * 
 */
public interface Message {

    public Integer getId();

    public Message setId(Integer id);

    public Object getBody();

    // public void setGuid(Object guid);

    public Message setBody(Object body);

}
