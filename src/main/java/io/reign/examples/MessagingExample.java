package io.reign.examples;

import io.reign.Reign;
import io.reign.messaging.DefaultMessagingService;
import io.reign.messaging.ResponseMessage;
import io.reign.messaging.SimpleRequestMessage;
import io.reign.presence.PresenceService;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Demonstrate basic usage.
 * 
 * @author ypai
 * 
 */
public class MessagingExample {

    private static final Logger logger = LoggerFactory.getLogger(MessagingExample.class);

    public static void main(String[] args) throws Exception {
        /** init and start reign using builder **/
        Reign reign = Reign.maker().zkClient("localhost:2181", 30000).pathCache(1024, 8).core().build();
        reign.start();

        /** messaging example **/
        messagingExample(reign);

        /** sleep to allow examples to run for a bit **/
        Thread.sleep(600000);

        /** shutdown reign **/
        reign.stop();

        /** sleep a bit to observe observer callbacks **/
        Thread.sleep(10000);
    }

    public static void messagingExample(Reign reign) throws Exception {
        PresenceService presenceService = reign.getService("presence");
        presenceService.announce("examples", "service1", true);
        presenceService.announce("examples", "service2", true);

        presenceService.waitUntilAvailable("examples", "service1", 30000);

        Thread.sleep(5000);

        DefaultMessagingService messagingService = reign.getService("messaging");

        Map<String, ResponseMessage> responseMap = messagingService.sendMessage("examples", "service1",
                new SimpleRequestMessage("presence", "/"));

        logger.info("Broadcast#1:  responseMap={}", responseMap);

        responseMap = messagingService.sendMessage("examples", "service1", new SimpleRequestMessage("presence",
                "/examples/service1"));

        logger.info("Broadcast#2:  responseMap={}", responseMap);

        responseMap = messagingService.sendMessage("examples", "service1", new SimpleRequestMessage("presence",
                "/examples"));

        logger.info("Broadcast#3:  responseMap={}", responseMap);
    }
}
