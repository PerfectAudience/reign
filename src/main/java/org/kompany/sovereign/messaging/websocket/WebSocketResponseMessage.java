package org.kompany.sovereign.messaging.websocket;

import org.jboss.netty.channel.Channel;
import org.kompany.sovereign.messaging.SimpleResponseMessage;

/**
 * 
 * @author ypai
 * 
 */
public class WebSocketResponseMessage extends SimpleResponseMessage {
    private Channel channel;

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }
}
