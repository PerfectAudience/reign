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

import io.reign.mesg.DefaultMessageProtocol;
import io.reign.mesg.MessagingProviderCallback;
import io.reign.mesg.NullMessagingProviderCallback;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;
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

    private String clusterId;
    private String serviceId;
    private String nodeId;
    private final ExecutorService requestMonitoringExecutor;

    public WebSocketClient(String uriString, ExecutorService requestMonitoringExecutor) throws URISyntaxException {
        this.uri = new URI(uriString);
        this.requestMonitoringExecutor = requestMonitoringExecutor;
    }

    public WebSocketClient(URI uri, ExecutorService requestMonitoringExecutor) {
        this.uri = uri;
        this.requestMonitoringExecutor = requestMonitoringExecutor;
    }

    public WebSocketClient(String clusterId, String serviceId, String nodeId, Channel channel,
            ExecutorService requestMonitoringExecutor) {
        this.channel = channel;
        handler = new WebSocketClientHandler(null);
        this.clusterId = clusterId;
        this.serviceId = serviceId;
        this.nodeId = nodeId;
        this.requestMonitoringExecutor = requestMonitoringExecutor;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public String getServiceId() {
        return serviceId;
    }

    public void setServiceId(String serviceId) {
        this.serviceId = serviceId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setCanonicalId(String nodeId) {
        this.nodeId = nodeId;
    }

    public synchronized void connect() throws Exception {
        if (channel != null && channel.isConnected()) {
            return;
        }

        logger.info("Connecting:  uri={}", uri);

        // HashMap<String, String> customHeaders = new HashMap<String, String>();
        // customHeaders.put("MyHeader", "MyValue");

        // Connect with V13 (RFC 6455 aka HyBi-17). You can change it to V08 or V00.
        // If you change it to V00, ping is not supported and remember to change
        // HttpResponseDecoder to WebSocketHttpResponseDecoder in the pipeline.
        final WebSocketClientHandshaker handshaker = new WebSocketClientHandshakerFactory().newHandshaker(uri,
                WebSocketVersion.V13, null, false, null);

        handler = new WebSocketClientHandler(handshaker);

        bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()));

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

    public void write(String text, final MessagingProviderCallback callback) {

        final int requestId = messageIdSequence.incrementAndGet();

        boolean fireAndForget = callback instanceof NullMessagingProviderCallback;
        if (!fireAndForget) {
            handler.registerCallback(channel, requestId, callback);
        }

        final ChannelFuture channelFuture = channel.write(new TextWebSocketFrame(text
                + (!fireAndForget ? " " + DefaultMessageProtocol.MESSAGE_ID_DELIMITER + " " + requestId : "")));

        if (fireAndForget) {
            return;
        }

        channelFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) {
                synchronized (channelFuture) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("notify() called:  handler.hashCode={}; channelFuture.hashCode()={}",
                                handler.hashCode(), channelFuture.hashCode());
                    }
                    channelFuture.notify();
                }
            }
        });

        requestMonitoringExecutor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    synchronized (channelFuture) {
                        try {
                            channelFuture.await(500);
                        } catch (InterruptedException e) {
                            if (logger.isTraceEnabled()) {
                                logger.trace("Interrupted:  handler.hashCode={}; channelFuture.hashCode()={}",
                                        handler.hashCode(), channelFuture.hashCode());
                            }
                        }
                    }
                    if (!channelFuture.isSuccess()) {
                        callback.error(null);
                        boolean cancelled = channelFuture.cancel();
                        logger.warn("Attempted cancel because no response:  channelFuture.hashCode()={}; cancelled={}",
                                channelFuture.hashCode(), cancelled);
                    } else {
                        int waitMillis = 1;
                        synchronized (callback) {
                            while (waitMillis < 65) {
                                try {
                                    callback.wait(waitMillis = waitMillis * 2);
                                    if (handler.getCallback(channel, requestId) == null) {
                                        if (logger.isTraceEnabled()) {
                                            logger.trace(
                                                    "Completed:  handler.hashCode={}; requestId={}; timeMillis={}",
                                                    handler.hashCode(), requestId, waitMillis);
                                        }
                                        return;
                                    }
                                } catch (InterruptedException e) {
                                    if (logger.isTraceEnabled()) {
                                        logger.trace("Interrupted:  handler.hashCode={}; requestId={}",
                                                handler.hashCode(), requestId);
                                    }
                                }
                            }
                        }
                        callback.response((String) null);
                        if (logger.isTraceEnabled()) {
                            logger.trace(
                                    "Removed callback for successful request with no response:  handler.hashCode={}; requestId={}",
                                    handler.hashCode(), requestId);
                        }
                    }
                } finally {
                    handler.removeCallback(channel, requestId);
                }
            }
        });

    }

    public void write(byte[] bytes, final MessagingProviderCallback callback) {
        throw new UnsupportedOperationException("Not yet supported.");
    }

    public void ping() {
        logger.debug("WebSocket Client sending ping");
        channel.write(new PingWebSocketFrame(ChannelBuffers.copiedBuffer(new byte[] { 1 })));
    }

    public synchronized void close() {
        try {
            if (channel.isConnected()) {
                logger.debug("WebSocket Client sending close");
                channel.write(new CloseWebSocketFrame());
            }

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

    public synchronized boolean isClosed() {
        return !channel.isConnected();
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
