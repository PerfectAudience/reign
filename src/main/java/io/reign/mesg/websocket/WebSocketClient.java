package io.reign.mesg.websocket;

import io.reign.mesg.DefaultMessageProtocol;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpRequestEncoder;
import org.jboss.netty.handler.codec.http.HttpResponseDecoder;
import org.jboss.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import org.jboss.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import org.jboss.netty.handler.codec.http.websocketx.WebSocketVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class WebSocketClient {

    private static final Logger logger = LoggerFactory.getLogger(WebSocketClient.class);

    private URI uri = null;

    private ChannelFuture future;

    private Channel channel;

    private WebSocketClientHandler handler;

    private ClientBootstrap bootstrap;

    private final AtomicInteger messageIdSequence = new AtomicInteger(0);

    public WebSocketClient(String uriString) throws URISyntaxException {
        this.uri = new URI(uriString);
    }

    public WebSocketClient(URI uri) {
        this.uri = uri;
    }

    public void connect() throws Exception {

        logger.info("Connecting:  uri={}", uri);

        // HashMap<String, String> customHeaders = new HashMap<String, String>();
        // customHeaders.put("MyHeader", "MyValue");

        // Connect with V13 (RFC 6455 aka HyBi-17). You can change it to V08 or V00.
        // If you change it to V00, ping is not supported and remember to change
        // HttpResponseDecoder to WebSocketHttpResponseDecoder in the pipeline.
        final WebSocketClientHandshaker handshaker = new WebSocketClientHandshakerFactory().newHandshaker(uri,
                WebSocketVersion.V13, null, false, null);

        handler = new WebSocketClientHandler(handshaker);

        bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors
                .newCachedThreadPool()));

        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();

                pipeline.addLast("decoder", new HttpResponseDecoder());
                pipeline.addLast("encoder", new HttpRequestEncoder());
                pipeline.addLast("ws-handler", handler);
                return pipeline;
            }
        });

        logger.debug("WebSocket Client connecting");
        future = bootstrap.connect(new InetSocketAddress(uri.getHost(), uri.getPort()));
        future.syncUninterruptibly();
        channel = future.getChannel();

        handshaker.handshake(channel).syncUninterruptibly();
    }

    public String write(String text) {

        final int requestId = messageIdSequence.incrementAndGet();

        final ChannelFuture channelFuture = channel.write(new TextWebSocketFrame(text
                + DefaultMessageProtocol.MESSAGE_ID_DELIMITER + requestId));
        channelFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) {
                synchronized (channelFuture) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("notify() called:  channelFuture.hashCode()={}", channelFuture.hashCode());
                    }
                    channelFuture.notify();
                }
            }
        });
        channelFuture.awaitUninterruptibly();

        Object response = null;
        synchronized (channelFuture) {
            try {
                int waitMillis = 2;
                while ((response = handler.pollResponse(requestId)) == null && waitMillis < 4096) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Calling wait({}):  requestId={}; channelFuture.hashCode()={}", new Object[] {
                                waitMillis, requestId, channelFuture.hashCode() });
                    }
                    channelFuture.wait(waitMillis);
                    waitMillis = waitMillis * 2;
                }
            } catch (InterruptedException e) {
                logger.warn("Interrupted while waiting for response:  " + e, e);
            }
        }

        if (logger.isTraceEnabled()) {
            logger.trace("Got response:  requestId={}; channelFuture.hashCode()={}; response={}", new Object[] {
                    requestId, text, response });
        }

        return (String) response;
    }

    public byte[] write(byte[] bytes) {
        throw new UnsupportedOperationException("Not yet supported.");
    }

    public void ping() {
        logger.debug("WebSocket Client sending ping");
        channel.write(new PingWebSocketFrame(ChannelBuffers.copiedBuffer(new byte[] { 1 })));
    }

    public void close() {
        try {
            logger.debug("WebSocket Client sending close");
            channel.write(new CloseWebSocketFrame());

            // WebSocketClientHandler will close the connection when the server
            // responds to the CloseWebSocketFrame.
            channel.getCloseFuture().awaitUninterruptibly();
        } finally {
            if (channel != null) {
                channel.close();
            }
        }
        bootstrap.releaseExternalResources();
    }

    // public void run() throws Exception {
    // ClientBootstrap bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors
    // .newCachedThreadPool(), Executors.newCachedThreadPool()));
    //
    // Channel ch = null;
    //
    // try {
    // String protocol = uri.getScheme();
    // if (!"ws".equals(protocol)) {
    // throw new IllegalArgumentException("Unsupported protocol: " + protocol);
    // }
    //
    // HashMap<String, String> customHeaders = new HashMap<String, String>();
    // customHeaders.put("MyHeader", "MyValue");
    //
    // // Connect with V13 (RFC 6455 aka HyBi-17). You can change it to V08 or V00.
    // // If you change it to V00, ping is not supported and remember to change
    // // HttpResponseDecoder to WebSocketHttpResponseDecoder in the pipeline.
    // final WebSocketClientHandshaker handshaker = new WebSocketClientHandshakerFactory().newHandshaker(uri,
    // WebSocketVersion.V13, null, false, customHeaders);
    //
    // bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
    // @Override
    // public ChannelPipeline getPipeline() throws Exception {
    // ChannelPipeline pipeline = Channels.pipeline();
    //
    // pipeline.addLast("decoder", new HttpResponseDecoder());
    // pipeline.addLast("encoder", new HttpRequestEncoder());
    // pipeline.addLast("ws-handler", new WebSocketClientHandler(handshaker));
    // return pipeline;
    // }
    // });
    //
    // // Connect
    // logger.debug("WebSocket Client connecting");
    // ChannelFuture future = bootstrap.connect(new InetSocketAddress(uri.getHost(), uri.getPort()));
    // future.syncUninterruptibly();
    //
    // ch = future.getChannel();
    // handshaker.handshake(ch).syncUninterruptibly();
    //
    // // Send 10 messages and wait for responses
    // logger.debug("WebSocket Client sending message");
    // for (int i = 0; i < 1000; i++) {
    // ch.write(new TextWebSocketFrame("Message #" + i));
    // }
    //
    // // Ping
    // logger.debug("WebSocket Client sending ping");
    // ch.write(new PingWebSocketFrame(ChannelBuffers.copiedBuffer(new byte[] { 1, 2, 3, 4, 5, 6 })));
    //
    // // Close
    // logger.debug("WebSocket Client sending close");
    // ch.write(new CloseWebSocketFrame());
    //
    // // WebSocketClientHandler will close the connection when the server
    // // responds to the CloseWebSocketFrame.
    // ch.getCloseFuture().awaitUninterruptibly();
    // } finally {
    // if (ch != null) {
    // ch.close();
    // }
    // bootstrap.releaseExternalResources();
    // }
    // }

}
