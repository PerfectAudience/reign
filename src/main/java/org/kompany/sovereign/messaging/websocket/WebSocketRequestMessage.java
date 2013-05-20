package org.kompany.sovereign.messaging.websocket;

import org.jboss.netty.channel.Channel;
import org.kompany.sovereign.messaging.SimpleRequestMessage;

/**
 * 
 * @author ypai
 * 
 */
public class WebSocketRequestMessage extends SimpleRequestMessage {

    private Channel channel;

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

}
