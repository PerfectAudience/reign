package org.kompany.sovereign.messaging.websocket;

import org.kompany.sovereign.MessagingProvider;

/**
 * 
 * @author ypai
 * 
 */
public class WebSocketsMessagingProvider implements MessagingProvider {

    private int port;

    private WebSocketServer server;

    @Override
    public void setPort(int port) {
        this.port = port;

    }

    @Override
    public void start() {
        this.server = new WebSocketServer(port);
        server.start();
    }

    @Override
    public void stop() {
        server.stop();
    }

}
