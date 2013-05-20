package org.kompany.sovereign.messaging;

/**
 * 
 * @author ypai
 * 
 */
public class SimpleResponseMessage extends AbstractMessage implements ResponseMessage {

    public SimpleResponseMessage() {
    }

    public SimpleResponseMessage(Object body) {
        setBody(body);
    }
}
