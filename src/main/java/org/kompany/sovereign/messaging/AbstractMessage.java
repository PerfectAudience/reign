package org.kompany.sovereign.messaging;

/**
 * 
 * @author ypai
 * 
 */
public abstract class AbstractMessage implements Message {

    private Object guid;

    private Object body;

    @Override
    public Object getGuid() {
        return guid;
    }

    @Override
    public void setGuid(Object guid) {
        this.guid = guid;
    }

    public Object getBody() {
        return body;
    }

    @Override
    public void setBody(Object body) {
        this.body = body;
    }

}
