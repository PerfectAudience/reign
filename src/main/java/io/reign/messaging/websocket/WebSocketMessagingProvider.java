package io.reign.messaging.websocket;

import io.reign.ReignContext;
import io.reign.UnexpectedReignException;
import io.reign.messaging.DefaultMessageProtocol;
import io.reign.messaging.MessageProtocol;
import io.reign.messaging.MessagingProvider;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class WebSocketMessagingProvider implements MessagingProvider {

    private static final Logger logger = LoggerFactory.getLogger(WebSocketMessagingProvider.class);

    private int port;

    private WebSocketServer server;

    private volatile boolean shutdown = true;

    // private ExecutorService executorService;

    private ReignContext serviceDirectory;

    private MessageProtocol messageProtocol;

    private final ConcurrentMap<String, WebSocketClient> clientMap = new ConcurrentHashMap<String, WebSocketClient>(32,
            0.9f, 2);

    @Override
    public String sendMessage(String hostOrIpAddress, int port, String message) {
        String endpointUri = "ws://" + hostOrIpAddress + ":" + port + "/websocket";
        WebSocketClient client = getClient(endpointUri);
        return client.write(message);
    }

    @Override
    public byte[] sendMessage(String hostOrIpAddress, int port, byte[] message) {
        String endpointUri = "ws://" + hostOrIpAddress + ":" + port + "/websocket";
        WebSocketClient client = getClient(endpointUri);
        return client.write(message);
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

    WebSocketClient getClient(String endpointUri) {
        WebSocketClient client = clientMap.get(endpointUri);
        if (client == null) {
            try {
                WebSocketClient newClient = new WebSocketClient(endpointUri);
                client = clientMap.putIfAbsent(endpointUri, newClient);
                if (client == null) {
                    client = newClient;
                    newClient.connect();
                }
            } catch (Exception e) {
                throw new UnexpectedReignException(e);
            }
        }// if
        return client;
    }

    @Override
    public synchronized void start() {
        if (!shutdown) {
            return;
        }

        if (messageProtocol == null) {
            logger.info("START:  using default message protocol");
            messageProtocol = new DefaultMessageProtocol();
        }

        logger.info("START:  starting websockets server");
        this.server = new WebSocketServer(port, serviceDirectory, messageProtocol);
        server.start();

        // logger.info("START:  initializing executor");
        // this.executorService = Executors.newFixedThreadPool(2);

        shutdown = false;

    }

    @Override
    public synchronized void stop() {
        if (shutdown) {
            return;
        }

        logger.info("STOP:  shutting down websockets server");
        server.stop();

        // logger.info("STOP:  shutting down executor");
        // if (this.executorService != null) {
        // executorService.shutdown();
        // }

        this.shutdown = true;
    }

}
