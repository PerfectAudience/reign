package io.reign.mesg.websocket;

import io.reign.ReignContext;
import io.reign.mesg.MessageProtocol;
import io.reign.util.IdUtil;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class WebSocketServer {

    private static final Logger logger = LoggerFactory.getLogger(WebSocketServer.class);

    private final int port;

    private ServerBootstrap bootstrap;

    private final ReignContext serviceDirectory;
    private final MessageProtocol messageProtocol;

    public WebSocketServer(int port, ReignContext serviceDirectory, MessageProtocol messageProtocol) {
        this.port = port;
        this.serviceDirectory = serviceDirectory;
        this.messageProtocol = messageProtocol;
    }

    public void start() {
        // Configure the server.
        bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors
                .newCachedThreadPool()));

        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory(new WebSocketServerPipelineFactory(serviceDirectory, messageProtocol));

        // Bind and start to accept incoming connections.
        bootstrap.bind(new InetSocketAddress(port));

        logger.info("Web socket server started at ws://{}:{}", IdUtil.getHostname(), port);
    }

    public void stop() {
        try {
            bootstrap.shutdown();
        } finally {
            bootstrap.releaseExternalResources();
        }
    }

}