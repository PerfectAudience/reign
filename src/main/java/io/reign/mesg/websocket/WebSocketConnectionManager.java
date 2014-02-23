package io.reign.mesg.websocket;

import io.reign.ReignContext;
import io.reign.presence.PresenceService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebSocketConnectionManager {
    private static final Logger logger = LoggerFactory.getLogger(WebSocketConnectionManager.class);

    private final ConcurrentMap<String, WebSocketClient> clientMap = new ConcurrentHashMap<String, WebSocketClient>(32,
            0.9f, 2);

    private volatile boolean shutdown = false;

    private ReignContext reignContext;

    private ExecutorService requestMonitoringExecutor;

    /**
     * How often to ping and check that a connection is still open
     */
    private long connectionTimeout = 10000;

    public ExecutorService getRequestMonitoringExecutor() {
        return requestMonitoringExecutor;
    }

    public void setRequestMonitoringExecutor(ExecutorService requestMonitoringExecutor) {
        this.requestMonitoringExecutor = requestMonitoringExecutor;
    }

    public void shutdown() {
        this.shutdown = true;
    }

    public ReignContext getReignContext() {
        return reignContext;
    }

    public void setReignContext(ReignContext reignContext) {
        this.reignContext = reignContext;
    }

    public long getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(long connectionTimeout) {
        if (connectionTimeout < 1000) {
            logger.warn("{}ms is too low:  defaulting connectionTimeout to 1000", connectionTimeout);
            connectionTimeout = 1000;
        }
        this.connectionTimeout = connectionTimeout;
    }

    public void init() {
        // start connection maintenance thread
        Thread adminThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!shutdown) {
                    for (String key : clientMap.keySet()) {
                        WebSocketClient client = clientMap.get(key);

                        if (!client.isClosed()) {
                            logger.debug("Sending PING:  remote={}", key);
                            client.ping();
                        } else {
                            logger.info("Connection has been closed:  remote={}", key);

                            // remove from map if client closed
                            logger.info("Removing connection:  remote={}", key);
                            clientMap.remove(key);

                            // remove presence node
                            if (client.getClusterId() != null && client.getServiceId() != null
                                    && client.getNodeId() != null) {
                                logger.info(
                                        "Removing presence node:  clusterId={}; serviceId={}; nodeId={}",
                                        new Object[] { client.getClusterId(), client.getServiceId(), client.getNodeId() });
                                PresenceService presenceService = reignContext.getService("presence");
                                presenceService.dead(client.getClusterId(), client.getServiceId(), client.getNodeId());
                            }
                        }
                    }

                    try {
                        Thread.sleep(connectionTimeout);
                    } catch (InterruptedException e) {
                        logger.warn("Interrupted:  " + e, e);
                    }
                }// while

                logger.info("STOP:  closing client connections");
                for (String key : clientMap.keySet()) {
                    WebSocketClient client = clientMap.get(key);

                    logger.info("STOP:  closing client connection:  remote={}", key);
                    client.close();
                }
            }
        });
        adminThread.setPriority(Thread.MIN_PRIORITY);
        adminThread.setDaemon(true);
        adminThread.setName("Reign:" + this.getClass().getSimpleName() + ".adminThread");
        adminThread.start();
    }

    public void addClientConnection(String remoteAddress, int remotePort, WebSocketClient webSocketClient) {
        String endpointUri = "ws://" + remoteAddress + ":" + remotePort + WebSocketMessagingProvider.WEBSOCKET_PATH;
        logger.info("Adding connection:  remote={}", endpointUri);
        clientMap.put(endpointUri, webSocketClient);
    }

    synchronized WebSocketClient getConnection(String endpointUri) {
        WebSocketClient client = clientMap.get(endpointUri);
        if (client == null) {
            try {
                WebSocketClient newClient = new WebSocketClient(endpointUri, requestMonitoringExecutor);
                client = clientMap.putIfAbsent(endpointUri, newClient);
                if (client == null) {
                    logger.info("Establishing connection:  remote={}", endpointUri);
                    client = newClient;
                    newClient.connect();
                }
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }

        }// if
        return client;
    }
}
