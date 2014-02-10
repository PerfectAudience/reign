package io.reign.mesg.websocket;

import java.util.concurrent.ConcurrentHashMap;
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

/**
 * 
 * @author ypai
 * 
 */
public class WebSocketClientHandler extends SimpleChannelUpstreamHandler {

    private static final Logger logger = LoggerFactory.getLogger(WebSocketClientHandler.class);

    /** Can be null if no handshaking is necessary */
    private final WebSocketClientHandshaker handshaker;

    private final ConcurrentMap<Integer, Object> responseHolder = new ConcurrentHashMap<Integer, Object>(64, 0.9f, 8);

    /**
     * @param handshaker
     *            can be null if no handshaking is required (already an established connection)
     */
    public WebSocketClientHandler(WebSocketClientHandshaker handshaker) {
        this.handshaker = handshaker;
    }

    public Object pollResponse(int requestId) {
        if (logger.isTraceEnabled()) {
            logger.trace("responseHolder.size()={}", responseHolder.size());
        }
        return responseHolder.remove(requestId);
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
                logger.trace("WebSocket Client received message: channelId={}; requestId={}; message='{}'",
                        new Object[] { ctx.getChannel().getId(), responseText });
            }

            // TODO: fix exceptions when browser clients don't send all assumed data
            String requestIdString = null;
            int requestId = 0;
            try {
                int indexLeftQuote = responseText.indexOf("\"id\":");
                int indexRightQuote = responseText.indexOf(",", indexLeftQuote);
                requestIdString = responseText.substring(indexLeftQuote + 5, indexRightQuote);
                if (requestIdString != null) {
                    requestId = Integer.parseInt(requestIdString);
                }
            } catch (Exception e1) {
                logger.warn("Fix this later:  " + e1, e1);
            }

            if (logger.isTraceEnabled()) {
                logger.trace("WebSocket Client received message: channelId={}; requestId={}; message='{}'",
                        new Object[] { ctx.getChannel().getId(), requestIdString, responseText });
            }

            responseHolder.put(requestId, responseText);

        } else if (frame instanceof PongWebSocketFrame) {
            logger.debug("WebSocket Client received pong");

        } else if (frame instanceof CloseWebSocketFrame) {
            logger.debug("WebSocket Client received closing");

            ch.close();

        } else if (frame instanceof PingWebSocketFrame) {
            logger.debug("WebSocket Client received ping, response with pong");

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