package io.reign.mesg.websocket;

import io.reign.mesg.SimpleResponseMessage;

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
