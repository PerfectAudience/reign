package io.reign.messaging.websocket;

import io.reign.messaging.SimpleRequestMessage;

import org.jboss.netty.channel.Channel;

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
