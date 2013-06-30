package io.reign.mesg.websocket;

import io.reign.mesg.SimpleRequestMessage;

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
