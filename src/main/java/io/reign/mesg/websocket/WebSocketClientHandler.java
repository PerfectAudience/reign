/*
 Copyright 2013 Yen Pai ypai@reign.io

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package io.reign.mesg.websocket;

import io.reign.mesg.MessagingProviderCallback;

import java.util.concurrent.ConcurrentMap;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import org.jboss.netty.handler.codec.http.websocketx.WebSocketFrame;
import org.jboss.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;

/**
 * 
 * @author ypai
 * 
 */
public class WebSocketClientHandler extends SimpleChannelUpstreamHandler {

    private static final Logger logger = LoggerFactory.getLogger(WebSocketClientHandler.class);

    /** Can be null if no handshaking is necessary */
    private final WebSocketClientHandshaker handshaker;

    private final ConcurrentMap<Integer, MessagingProviderCallback> responseHolder = new ConcurrentLinkedHashMap.Builder<Integer, MessagingProviderCallback>()
            .maximumWeightedCapacity(64).initialCapacity(64).concurrencyLevel(8)
            .listener(new EvictionListener<Integer, MessagingProviderCallback>() {
                @Override
                public void onEviction(Integer requestId, MessagingProviderCallback callback) {
                    callback.error(null);
                }
            }).build();

    /**
     * @param handshaker
     *            can be null if no handshaking is required (already an established connection)
     */
    public WebSocketClientHandler(WebSocketClientHandshaker handshaker) {
        this.handshaker = handshaker;
    }

    public void registerCallback(Channel channel, int requestId, MessagingProviderCallback callback) {
        responseHolder.put(requestId, callback);
        if (logger.isTraceEnabled()) {
            logger.trace(
                    "Registered callback:  hashCode={}; requestId={}; responseHolder.size()={}; responseHolder.keySet()={}; remoteAddress={}",
                    this.hashCode(), requestId, responseHolder.size(), responseHolder.keySet(), channel
                            .getRemoteAddress().toString());
        }

    }

    public MessagingProviderCallback getCallback(Channel channel, int requestId) {
        MessagingProviderCallback callback = responseHolder.get(requestId);
        // if (logger.isTraceEnabled()) {
        // logger.trace(
        // "Retrieving callback:  hashCode={}; requestId={}; responseHolder.size()={}; responseHolder.keySet()={}; remoteAddress={}",
        // this.hashCode(), requestId, responseHolder.size(), responseHolder.keySet(), channel
        // .getRemoteAddress().toString());
        // }
        return callback;
    }

    public MessagingProviderCallback removeCallback(Channel channel, int requestId) {
        MessagingProviderCallback removed = responseHolder.remove(requestId);
        if (logger.isTraceEnabled()) {
            logger.trace(
                    "Removed callback:  hashCode={}; requestId={}; responseHolder.size()={}; responseHolder.keySet()={}; remoteAddress={}",
                    this.hashCode(), requestId, responseHolder.size(), responseHolder.keySet(), channel
                            .getRemoteAddress().toString());
        }
        return removed;
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        logger.debug("WebSocket Client disconnected!");
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

        Channel ch = ctx.getChannel();
        if (!handshaker.isHandshakeComplete()) {
            handshaker.finishHandshake(ch, (HttpResponse) e.getMessage());
            logger.debug("WebSocket Client connected!");
            return;
        }

        if (e.getMessage() instanceof HttpResponse) {
            HttpResponse response = (HttpResponse) e.getMessage();
            throw new Exception("Unexpected HttpResponse (status=" + response.getStatus() + ", content="
                    + response.getContent().toString(CharsetUtil.UTF_8) + ')');
        }

        WebSocketFrame frame = (WebSocketFrame) e.getMessage();
        if (frame instanceof TextWebSocketFrame) {
            TextWebSocketFrame textFrame = (TextWebSocketFrame) frame;

            String responseText = textFrame.getText();

            if (logger.isTraceEnabled()) {
                logger.trace("Received message: hashCode={}; channelId={}; message='{}'; remoteAddress={}",
                        this.hashCode(), ctx.getChannel().getId(), responseText, ch.getRemoteAddress().toString());
            }

            String requestIdString = null;
            Integer requestId = null;
            try {
                int indexLeftQuote = responseText.indexOf("\"id\":");
                if (indexLeftQuote != -1) {
                    int indexRightQuote = responseText.indexOf(",", indexLeftQuote);
                    requestIdString = responseText.substring(indexLeftQuote + 5, indexRightQuote);
                    if (requestIdString != null) {
                        requestId = Integer.parseInt(requestIdString);
                    }
                }
            } catch (Exception e1) {
                logger.warn("Unexpected response:  " + e1 + ":  message='" + responseText + "'; remoteAddress="
                        + ch.getRemoteAddress().toString(), e1);
            }

            if (requestId != null) {
                MessagingProviderCallback messagingProviderCallback = responseHolder.remove(requestId);
                if (messagingProviderCallback != null) {
                    messagingProviderCallback.response(responseText);
                    synchronized (messagingProviderCallback) {
                        messagingProviderCallback.notifyAll();
                    }
                    if (logger.isTraceEnabled()) {
                        logger.trace(
                                "Invoked callback:  hashCode={}; requestId={}; responseHolder.size()={}; responseHolder.keySet()={}; remoteAddress={}",
                                this.hashCode(), requestId, responseHolder.size(), responseHolder.keySet(), ch
                                        .getRemoteAddress().toString());
                    }
                } else {
                    if (logger.isTraceEnabled()) {
                        logger.trace(
                                "No callback found:  hashCode={}; requestId={}; responseHolder.size()={}; responseHolder.keySet()={}; remoteAddress={}",
                                this.hashCode(), requestId, responseHolder.size(), responseHolder.keySet(), ch
                                        .getRemoteAddress().toString());
                    }
                }
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace(
                            "No requestId:  hashCode={}; requestId={}; responseHolder.size()={}; responseHolder.keySet()={}; remoteAddress={}",
                            this.hashCode(), requestId, responseHolder.size(), responseHolder.keySet(), ch
                                    .getRemoteAddress().toString());
                }
            }

        } else if (frame instanceof PongWebSocketFrame) {
            logger.trace("Received pong");

        } else if (frame instanceof CloseWebSocketFrame) {
            logger.trace("Received closing");

            ch.close();

        } else if (frame instanceof PingWebSocketFrame) {
            logger.trace("Received ping:  respond with pong");

            ch.write(new PongWebSocketFrame(frame.getBinaryData()));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        final Throwable t = e.getCause();
        t.printStackTrace();
        e.getChannel().close();
    }
}