package io.reign.mesg;

/**
 * 
 * @author ypai
 * 
 */
public interface Message {

    public Integer getId();

    public Message setId(Integer id);

    public Object getBody();

    public Message setBody(Object body);

}
