package io.reign.messaging.websocket;

import io.reign.messaging.SimpleResponseMessage;

import org.jboss.netty.channel.Channel;

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
