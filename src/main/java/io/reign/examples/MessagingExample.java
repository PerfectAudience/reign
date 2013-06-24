package io.reign.examples;

import io.reign.Reign;
import io.reign.messaging.MessagingService;
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
        /** init and start sovereign using builder **/
        Reign reign = Reign.builder().zkClient("localhost:2181", 30000).pathCache(1024, 8).allCoreServices().build();
        reign.start();

        /** messaging example **/
        messagingExample(reign);

        /** sleep to allow examples to run for a bit **/
        Thread.sleep(600000);

        /** shutdown sovereign **/
        reign.stop();

        /** sleep a bit to observe observer callbacks **/
        Thread.sleep(10000);
    }

    public static void messagingExample(Reign reign) throws Exception {
        PresenceService presenceService = reign.getService("presence");
        presenceService.announce("examples", "service3", reign.getPathScheme().getCanonicalId(1234), true);
        presenceService.announce("examples", "service4", reign.getPathScheme().getCanonicalId(4321), true);

        presenceService.waitUntilAvailable("examples", "service3", 30000);

        Thread.sleep(5000);

        MessagingService messagingService = reign.getService("messaging");

        Map<String, ResponseMessage> responseMap = messagingService.sendMessage("examples", "service3",
                new SimpleRequestMessage("presence", "/"));

        logger.info("Broadcast#1:  responseMap={}", responseMap);

        responseMap = messagingService.sendMessage("examples", "service4", new SimpleRequestMessage("presence",
                "/examples/service3"));

        logger.info("Broadcast#2:  responseMap={}", responseMap);

        responseMap = messagingService.sendMessage("examples", "service3", new SimpleRequestMessage("presence",
                "/examples"));

        logger.info("Broadcast#3:  responseMap={}", responseMap);
    }
}
