package io.reign.messaging;

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
    public void setId(Integer id) {
        this.id = id;
    }

    @Override
    public Object getBody() {
        return body;
    }

    @Override
    public void setBody(Object body) {
        this.body = body;
    }

}
