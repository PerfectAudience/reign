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

import io.reign.ReignContext;
import io.reign.mesg.DefaultMessageProtocol;
import io.reign.mesg.MessageProtocol;
import io.reign.mesg.MessagingProvider;
import io.reign.mesg.MessagingProviderCallback;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

    private final ExecutorService requestMonitoringExecutor = new ThreadPoolExecutor(16, 16, 60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(100), new RejectedExecutionHandler() {

                @Override
                public void rejectedExecution(Runnable runnable, ThreadPoolExecutor executor) {
                    // just run in current thread
                    runnable.run();
                }

            });

    @Override
    public void sendMessage(String hostOrIpAddress, int port, String message, MessagingProviderCallback callback) {
        String endpointUri = "ws://" + hostOrIpAddress + ":" + port + WEBSOCKET_PATH;
        WebSocketClient client = connectionManager.getConnection(endpointUri);
        client.write(message, callback);
    }

    @Override
    public void sendMessage(String hostOrIpAddress, int port, byte[] message, MessagingProviderCallback callback) {
        String endpointUri = "ws://" + hostOrIpAddress + ":" + port + WEBSOCKET_PATH;
        WebSocketClient client = connectionManager.getConnection(endpointUri);
        client.write(message, callback);
    }

    // @Override
    // public String sendMessageForget(String hostOrIpAddress, int port, String message) {
    // String endpointUri = "ws://" + hostOrIpAddress + ":" + port + WEBSOCKET_PATH;
    // WebSocketClient client = connectionManager.getConnection(endpointUri);
    // return client.write(message, false);
    // }
    //
    // @Override
    // public byte[] sendMessageForget(String hostOrIpAddress, int port, byte[] message) {
    // String endpointUri = "ws://" + hostOrIpAddress + ":" + port + WEBSOCKET_PATH;
    // WebSocketClient client = connectionManager.getConnection(endpointUri);
    // return client.write(message, false);
    // }

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
        connectionManager.setRequestMonitoringExecutor(this.requestMonitoringExecutor);
        connectionManager.setReignContext(serviceDirectory);
        // connectionManager.setConnectionTimeout(presenceService.getHeartbeatIntervalMillis());
        connectionManager.init();

        if (messageProtocol == null) {
            logger.info("START:  using default message protocol");
            messageProtocol = new DefaultMessageProtocol();
        }

        logger.info("START:  starting websockets server");
        this.server = new WebSocketServer(port, serviceDirectory, connectionManager, messageProtocol,
                this.requestMonitoringExecutor);
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
