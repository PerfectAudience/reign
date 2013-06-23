package io.reign.messaging;

/**
 * 
 * @author ypai
 * 
 */
public interface Message {

    public Integer getId();

    public void setId(Integer id);

    public Object getBody();

    // public void setGuid(Object guid);

    public void setBody(Object body);

}
