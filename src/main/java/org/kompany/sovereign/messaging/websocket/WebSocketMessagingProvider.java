package org.kompany.sovereign.messaging.websocket;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.kompany.sovereign.ServiceDirectory;
import org.kompany.sovereign.messaging.DefaultMessageProtocol;
import org.kompany.sovereign.messaging.MessageProtocol;
import org.kompany.sovereign.messaging.MessageQueue;
import org.kompany.sovereign.messaging.MessagingProvider;
import org.kompany.sovereign.messaging.SimpleMessageQueue;
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

    private MessageQueue requestQueue;

    private MessageQueue responseQueue;

    private volatile boolean shutdown = true;

    private ExecutorService executorService;

    private ServiceDirectory serviceDirectory;

    private MessageProtocol messageProtocol;

    public MessageProtocol getMessageProtocol() {
        return messageProtocol;
    }

    public void setMessageProtocol(MessageProtocol messageProtocol) {
        this.messageProtocol = messageProtocol;
    }

    public ServiceDirectory getServiceDirectory() {
        return serviceDirectory;
    }

    @Override
    public void setServiceDirectory(ServiceDirectory serviceDirectory) {
        this.serviceDirectory = serviceDirectory;
    }

    public void setRequestQueue(MessageQueue requestQueue) {
        this.requestQueue = requestQueue;
    }

    public void setResponseQueue(MessageQueue responseQueue) {
        this.responseQueue = responseQueue;
    }

    @Override
    public MessageQueue getRequestQueue() {
        return requestQueue;
    }

    @Override
    public MessageQueue getResponseQueue() {
        return responseQueue;
    }

    public int getPort() {
        return this.port;
    }

    @Override
    public void setPort(int port) {
        this.port = port;

    }

    @Override
    public void start() {
        if (requestQueue == null) {
            logger.info("START:  using default in-memory queue for request queue");
            requestQueue = new SimpleMessageQueue();
        }
        if (responseQueue == null) {
            logger.info("START:  using default in-memory queue for response queue");
            responseQueue = new SimpleMessageQueue();
        }
        if (messageProtocol == null) {
            logger.info("START:  using default message protocol");
            messageProtocol = new DefaultMessageProtocol();
        }

        logger.info("START:  starting websockets server");
        this.server = new WebSocketServer(port, serviceDirectory, messageProtocol);
        server.start();

        shutdown = false;

        logger.info("START:  initializing executor");
        this.executorService = Executors.newFixedThreadPool(2);

        logger.info("START:  starting request servicing runnable");
        this.executorService.submit(new Runnable() {
            @Override
            public void run() {
                while (!shutdown) {
                    try {
                        getRequestQueue().poll(3, TimeUnit.SECONDS);

                    } catch (InterruptedException e) {
                        logger.warn("Interrupted while waiting for requests:  " + e, e);
                    }
                }// while
            }// run()
        });

        logger.info("START:  starting response servicing runnable");
        this.executorService.submit(new Runnable() {
            @Override
            public void run() {
                while (!shutdown) {
                    try {
                        getResponseQueue().poll(3, TimeUnit.SECONDS);

                    } catch (InterruptedException e) {
                        logger.warn("Interrupted while waiting for responses:  " + e, e);
                    }
                }// while
            }// run()
        });

    }

    @Override
    public void stop() {
        logger.info("STOP:  shutting down websockets server");
        server.stop();
        shutdown = true;

        logger.info("STOP:  shutting down executor");
        executorService.shutdown();
    }

}
