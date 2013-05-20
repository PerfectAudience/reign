/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.kompany.sovereign.messaging.websocket;

/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

import static org.jboss.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static org.jboss.netty.handler.codec.http.HttpHeaders.setContentLength;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.HOST;
import static org.jboss.netty.handler.codec.http.HttpMethod.GET;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ChildChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.WriteCompletionEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.WebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.WebSocketServerHandshaker;
import org.jboss.netty.handler.codec.http.websocketx.WebSocketServerHandshakerFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.jboss.netty.util.CharsetUtil;
import org.kompany.sovereign.Service;
import org.kompany.sovereign.ServiceDirectory;
import org.kompany.sovereign.messaging.MessageProtocol;
import org.kompany.sovereign.messaging.RequestMessage;
import org.kompany.sovereign.messaging.ResponseMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles handshakes and messages
 */
public class WebSocketServerHandler extends ExecutionHandler {
    private static final Logger logger = LoggerFactory.getLogger(WebSocketServerHandler.class);

    private static final String WEBSOCKET_PATH = "/websocket";

    private WebSocketServerHandshaker handshaker;

    private ServiceDirectory serviceDirectory;

    private MessageProtocol messageProtocol;

    public WebSocketServerHandler(ServiceDirectory serviceDirectory, MessageProtocol messageProtocol) {
        super(new OrderedMemoryAwareThreadPoolExecutor(8, 1048576, 8 * 1048576));
        this.serviceDirectory = serviceDirectory;
        this.messageProtocol = messageProtocol;
    }

    public ServiceDirectory getServiceDirectory() {
        return serviceDirectory;
    }

    public void setServiceDirectory(ServiceDirectory serviceDirectory) {
        this.serviceDirectory = serviceDirectory;
    }

    public MessageProtocol getMessageProtocol() {
        return messageProtocol;
    }

    public void setMessageProtocol(MessageProtocol messageProtocol) {
        this.messageProtocol = messageProtocol;
    }

    /**
     * {@inheritDoc} Down-casts the received upstream event into more meaningful sub-type event and calls an appropriate
     * handler method with the down-casted event.
     */
    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {

        if (e instanceof MessageEvent) {
            messageReceived(ctx, (MessageEvent) e);
        } else if (e instanceof WriteCompletionEvent) {
            WriteCompletionEvent evt = (WriteCompletionEvent) e;
            writeComplete(ctx, evt);
        } else if (e instanceof ChildChannelStateEvent) {
            ChildChannelStateEvent evt = (ChildChannelStateEvent) e;
            if (evt.getChildChannel().isOpen()) {
                childChannelOpen(ctx, evt);
            } else {
                childChannelClosed(ctx, evt);
            }
        } else if (e instanceof ChannelStateEvent) {
            ChannelStateEvent evt = (ChannelStateEvent) e;
            switch (evt.getState()) {
            case OPEN:
                if (Boolean.TRUE.equals(evt.getValue())) {
                    channelOpen(ctx, evt);
                } else {
                    channelClosed(ctx, evt);
                }
                break;
            case BOUND:
                if (evt.getValue() != null) {
                    channelBound(ctx, evt);
                } else {
                    channelUnbound(ctx, evt);
                }
                break;
            case CONNECTED:
                if (evt.getValue() != null) {
                    channelConnected(ctx, evt);
                } else {
                    channelDisconnected(ctx, evt);
                }
                break;
            case INTEREST_OPS:
                channelInterestChanged(ctx, evt);
                break;
            default:
                ctx.sendUpstream(e);
            }
        } else if (e instanceof ExceptionEvent) {
            exceptionCaught(ctx, (ExceptionEvent) e);
        } else {
            ctx.sendUpstream(e);
        }
    }

    // /**
    // * Invoked when a message object (e.g: {@link ChannelBuffer}) was received
    // * from a remote peer.
    // */
    // public void messageReceived(
    // ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    // ctx.sendUpstream(e);
    // }

    // /**
    // * Invoked when an exception was raised by an I/O thread or a
    // * {@link ChannelHandler}.
    // */
    // public void exceptionCaught(
    // ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    // ChannelPipeline pipeline = ctx.getPipeline();
    //
    // ChannelHandler last = pipeline.getLast();
    // if (!(last instanceof ChannelUpstreamHandler) && ctx instanceof DefaultChannelPipeline) {
    // // The names comes in the order of which they are insert when using DefaultChannelPipeline
    // List<String> names = ctx.getPipeline().getNames();
    // for (int i = names.size() - 1; i >= 0; i--) {
    // ChannelHandler handler = ctx.getPipeline().get(names.get(i));
    // if (handler instanceof ChannelUpstreamHandler) {
    // // find the last handler
    // last = handler;
    // break;
    // }
    // }
    // }
    // if (this == last) {
    // logger.warn(
    // "EXCEPTION, please implement " + getClass().getName() +
    // ".exceptionCaught() for proper handling.", e.getCause());
    // }
    // ctx.sendUpstream(e);
    // }

    /**
     * Invoked when a {@link Channel} is open, but not bound nor connected. <br/>
     * 
     * <strong>Be aware that this event is fired from within the Boss-Thread so you should not execute any heavy
     * operation in there as it will block the dispatching to other workers!</strong>
     */
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        ctx.sendUpstream(e);
    }

    /**
     * Invoked when a {@link Channel} is open and bound to a local address, but not connected. <br/>
     * 
     * <strong>Be aware that this event is fired from within the Boss-Thread so you should not execute any heavy
     * operation in there as it will block the dispatching to other workers!</strong>
     */
    public void channelBound(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        ctx.sendUpstream(e);
    }

    /**
     * Invoked when a {@link Channel} is open, bound to a local address, and connected to a remote address. <br/>
     * 
     * <strong>Be aware that this event is fired from within the Boss-Thread so you should not execute any heavy
     * operation in there as it will block the dispatching to other workers!</strong>
     */
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        ctx.sendUpstream(e);
    }

    /**
     * Invoked when a {@link Channel}'s {@link Channel#getInterestOps() interestOps} was changed.
     */
    public void channelInterestChanged(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        ctx.sendUpstream(e);
    }

    /**
     * Invoked when a {@link Channel} was disconnected from its remote peer.
     */
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        ctx.sendUpstream(e);
    }

    /**
     * Invoked when a {@link Channel} was unbound from the current local address.
     */
    public void channelUnbound(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        ctx.sendUpstream(e);
    }

    /**
     * Invoked when a {@link Channel} was closed and all its related resources were released.
     */
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        ctx.sendUpstream(e);
    }

    /**
     * Invoked when something was written into a {@link Channel}.
     */
    public void writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent e) throws Exception {
        ctx.sendUpstream(e);
    }

    /**
     * Invoked when a child {@link Channel} was open. (e.g. a server channel accepted a connection)
     */
    public void childChannelOpen(ChannelHandlerContext ctx, ChildChannelStateEvent e) throws Exception {
        ctx.sendUpstream(e);
    }

    /**
     * Invoked when a child {@link Channel} was closed. (e.g. the accepted connection was closed)
     */
    public void childChannelClosed(ChannelHandlerContext ctx, ChildChannelStateEvent e) throws Exception {
        ctx.sendUpstream(e);
    }

    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        Object msg = e.getMessage();
        if (msg instanceof HttpRequest) {
            handleHttpRequest(ctx, (HttpRequest) msg);
        } else if (msg instanceof WebSocketFrame) {
            handleWebSocketFrame(ctx, (WebSocketFrame) msg);
        }
    }

    private void handleHttpRequest(ChannelHandlerContext ctx, HttpRequest req) throws Exception {
        // Allow only GET methods.
        if (req.getMethod() != GET) {
            sendHttpResponse(ctx, req, new DefaultHttpResponse(HTTP_1_1, FORBIDDEN));
            return;
        }

        // Send the demo page and favicon.ico
        if ("/".equals(req.getUri())) {
            HttpResponse res = new DefaultHttpResponse(HTTP_1_1, OK);

            ChannelBuffer content = WebSocketServerIndexPage.getContent(getWebSocketLocation(req));

            res.setHeader(CONTENT_TYPE, "text/html; charset=UTF-8");
            setContentLength(res, content.readableBytes());

            res.setContent(content);
            sendHttpResponse(ctx, req, res);
            return;
        }
        if ("/favicon.ico".equals(req.getUri())) {
            HttpResponse res = new DefaultHttpResponse(HTTP_1_1, NOT_FOUND);
            sendHttpResponse(ctx, req, res);
            return;
        }

        // Handshake
        WebSocketServerHandshakerFactory wsFactory = new WebSocketServerHandshakerFactory(getWebSocketLocation(req),
                null, false);
        handshaker = wsFactory.newHandshaker(req);
        if (handshaker == null) {
            wsFactory.sendUnsupportedWebSocketVersionResponse(ctx.getChannel());
        } else {
            handshaker.handshake(ctx.getChannel(), req).addListener(WebSocketServerHandshaker.HANDSHAKE_LISTENER);
        }
    }

    private void handleWebSocketFrame(ChannelHandlerContext ctx, WebSocketFrame frame) {

        // Check for closing frame
        if (frame instanceof CloseWebSocketFrame) {
            handshaker.close(ctx.getChannel(), (CloseWebSocketFrame) frame);
            return;
        } else if (frame instanceof PingWebSocketFrame) {
            ctx.getChannel().write(new PongWebSocketFrame(frame.getBinaryData()));
            return;
        } else if (frame instanceof TextWebSocketFrame) {
            // could potentially be longer running task, so execute in a separate threadpool
            final ChannelHandlerContext finalCtx = ctx;
            final WebSocketFrame finalFrame = frame;
            this.getExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    String requestText = ((TextWebSocketFrame) finalFrame).getText();
                    RequestMessage requestMessage = getMessageProtocol().fromTextRequest(requestText);
                    if (requestMessage != null) {
                        Service targetService = getServiceDirectory().getService(requestMessage.getTargetService());
                        if (targetService != null) {
                            ResponseMessage responseMessage = targetService.handleMessage(requestMessage);
                            if (responseMessage != null) {
                                finalCtx.getChannel().write(
                                        new TextWebSocketFrame(getMessageProtocol().toTextResponse(responseMessage)));
                            } else {
                                logger.warn("No response for request:  request='{}'", requestText);
                            }
                        } else {
                            logger.warn(
                                    "Message targets unknown service:  request='{}'; requestMessage.targetService='{}'",
                                    requestText, requestMessage.getTargetService());
                        }// if
                    } else {
                        logger.warn("Received poorly formed message:  request='{}'", requestText);
                        finalCtx.getChannel().close();
                    }// if

                    // Send the uppercase string back.
                    // if (logger.isDebugEnabled()) {
                    // logger.debug(String.format("Channel %s received %s", finalCtx.getChannel().getId(), request));
                    // }
                    // finalCtx.getChannel().write(new TextWebSocketFrame(requestText.toUpperCase()));
                }
            });

        } else if (frame instanceof BinaryWebSocketFrame) {
            // could potentially be longer running task, so execute in a separate threadpool
            final ChannelHandlerContext finalCtx = ctx;
            final WebSocketFrame finalFrame = frame;
            this.getExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    finalCtx.getChannel().write(
                            new BinaryWebSocketFrame(finalFrame.isFinalFragment(), finalFrame.getRsv(), finalFrame
                                    .getBinaryData()));
                }
            });

        } else if (frame instanceof ContinuationWebSocketFrame) {
            // could potentially be longer running task, so execute in a separate threadpool
            final ChannelHandlerContext finalCtx = ctx;
            final WebSocketFrame finalFrame = frame;
            this.getExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    finalCtx.getChannel().write(
                            new ContinuationWebSocketFrame(finalFrame.isFinalFragment(), finalFrame.getRsv(),
                                    finalFrame.getBinaryData()));
                }
            });

        } else if (frame instanceof PongWebSocketFrame) {
            // Ignore
        } else {
            throw new UnsupportedOperationException(String.format("%s frame types not supported", frame.getClass()
                    .getName()));
        }

    }

    private static void sendHttpResponse(ChannelHandlerContext ctx, HttpRequest req, HttpResponse res) {
        // Generate an error page if response status code is not OK (200).
        if (res.getStatus().getCode() != 200) {
            res.setContent(ChannelBuffers.copiedBuffer(res.getStatus().toString(), CharsetUtil.UTF_8));
            setContentLength(res, res.getContent().readableBytes());
        }

        // Send the response and close the connection if necessary.
        ChannelFuture f = ctx.getChannel().write(res);
        if (!isKeepAlive(req) || res.getStatus().getCode() != 200) {
            f.addListener(ChannelFutureListener.CLOSE);
        }
    }

    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        e.getCause().printStackTrace();
        e.getChannel().close();
    }

    private static String getWebSocketLocation(HttpRequest req) {
        return "ws://" + req.getHeader(HOST) + WEBSOCKET_PATH;
    }
}