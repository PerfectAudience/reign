package io.reign.mesg;

/**
 * 
 * @author ypai
 * 
 */
public abstract class AbstractMessage implements Message {

    private Integer id = null;

    private Object body;

    @Override
    public Integer getId() {
        return id;
    }

    @Override
    public Message setId(Integer id) {
        this.id = id;
        return this;
    }

    @Override
    public Object getBody() {
        return body;
    }

    @Override
    public Message setBody(Object body) {
        this.body = body;
        return this;
    }

}
