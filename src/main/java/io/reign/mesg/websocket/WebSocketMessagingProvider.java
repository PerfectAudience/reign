package io.reign.mesg.websocket;

import io.reign.ReignContext;
import io.reign.mesg.DefaultMessageProtocol;
import io.reign.mesg.MessageProtocol;
import io.reign.mesg.MessagingProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class WebSocketMessagingProvider implements MessagingProvider {

    private static final Logger logger = LoggerFactory.getLogger(WebSocketMessagingProvider.class);

    public static final String WEBSOCKET_PATH = "/ws";

    private int port;

    private WebSocketServer server;

    private volatile boolean shutdown = true;

    private ReignContext serviceDirectory;

    private MessageProtocol messageProtocol;

    private WebSocketConnectionManager connectionManager;

    @Override
    public String sendMessage(String hostOrIpAddress, int port, String message) {
        String endpointUri = "ws://" + hostOrIpAddress + ":" + port + WEBSOCKET_PATH;
        WebSocketClient client = connectionManager.getConnection(endpointUri);
        return client.write(message, true);
    }

    @Override
    public byte[] sendMessage(String hostOrIpAddress, int port, byte[] message) {
        String endpointUri = "ws://" + hostOrIpAddress + ":" + port + WEBSOCKET_PATH;
        WebSocketClient client = connectionManager.getConnection(endpointUri);
        return client.write(message, true);
    }

    @Override
    public String sendMessageForget(String hostOrIpAddress, int port, String message) {
        String endpointUri = "ws://" + hostOrIpAddress + ":" + port + WEBSOCKET_PATH;
        WebSocketClient client = connectionManager.getConnection(endpointUri);
        return client.write(message, false);
    }

    @Override
    public byte[] sendMessageForget(String hostOrIpAddress, int port, byte[] message) {
        String endpointUri = "ws://" + hostOrIpAddress + ":" + port + WEBSOCKET_PATH;
        WebSocketClient client = connectionManager.getConnection(endpointUri);
        return client.write(message, false);
    }

    @Override
    public MessageProtocol getMessageProtocol() {
        return messageProtocol;
    }

    @Override
    public void setMessageProtocol(MessageProtocol messageProtocol) {
        this.messageProtocol = messageProtocol;
    }

    public ReignContext getServiceDirectory() {
        return serviceDirectory;
    }

    @Override
    public void setServiceDirectory(ReignContext serviceDirectory) {
        this.serviceDirectory = serviceDirectory;
    }

    @Override
    public int getPort() {
        return this.port;
    }

    @Override
    public void setPort(int port) {
        this.port = port;

    }

    @Override
    public synchronized void init() {
        if (!shutdown) {
            return;
        }

        // PresenceService presenceService = serviceDirectory.getService("presence");

        connectionManager = new WebSocketConnectionManager();
        connectionManager.setReignContext(serviceDirectory);
        // connectionManager.setConnectionTimeout(presenceService.getHeartbeatIntervalMillis());
        connectionManager.init();

        if (messageProtocol == null) {
            logger.info("START:  using default message protocol");
            messageProtocol = new DefaultMessageProtocol();
        }

        logger.info("START:  starting websockets server");
        this.server = new WebSocketServer(port, serviceDirectory, connectionManager, messageProtocol);
        server.start();

        shutdown = false;

    }

    @Override
    public synchronized void destroy() {
        if (shutdown) {
            return;
        }

        logger.info("STOP:  shutting down websockets server");
        server.stop();

        this.shutdown = true;
    }

}
