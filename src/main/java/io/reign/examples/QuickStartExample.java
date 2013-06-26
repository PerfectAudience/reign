package io.reign.examples;

import io.reign.Reign;
import io.reign.conf.ConfService;
import io.reign.coord.CoordinationService;
import io.reign.coord.DistributedReadWriteLock;
import io.reign.coord.DistributedReentrantLock;
import io.reign.messaging.MessagingService;
import io.reign.messaging.ResponseMessage;
import io.reign.messaging.SimpleRequestMessage;
import io.reign.presence.PresenceService;
import io.reign.util.Structs;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Demonstrates basic usage of core services.
 * 
 * @author ypai
 * 
 */
public class QuickStartExample {

    private static final Logger logger = LoggerFactory.getLogger(QuickStartExample.class);

    public static void main(String[] args) throws Exception {
        /** init and start reign using builder **/
        Reign reign = Reign.maker().zkClient("localhost:2181", 30000).allCoreServices().build();
        reign.start();

        /** presence service example **/
        // get the presence service
        PresenceService presenceService = reign.getService("presence");

        // announce this node's available for a given service, immediately visible
        presenceService.announce("examples", "service1", true);

        // announce this node's available for another service, not immediately visible
        presenceService.announce("examples", "service2", false);

        // hide service1
        presenceService.hide("examples", "service1");

        // show service2
        presenceService.show("examples", "service2");

        /** configuration service example **/
        // get the configuration service
        ConfService confService = (ConfService) reign.getService("conf");

        // store configuration as properties file
        Properties props = new Properties();
        props.setProperty("capacity.min", "111");
        props.setProperty("capacity.max", "999");
        props.setProperty("lastSavedTimestamp", System.currentTimeMillis() + "");
        confService.putConf("examples", "config1.properties", props);

        // retrieve configuration as properties file
        Properties loadedProperties = confService.getConf("examples", "config1.properties");

        // store configuration as JSON file
        Map<String, String> json = new HashMap<String, String>();
        confService.putConf(
                "examples",
                "config1.js",
                Structs.<String, String> map().kv("capacity.min", "222").kv("capacity.max", "888")
                        .kv("lastSavedTimestamp", System.currentTimeMillis() + ""));

        // retrieve configuration as JSON file
        Map<String, String> loadedJson = confService.getConf("examples", "config1.js");

        /** messaging example **/
        // get the messaging service
        MessagingService messagingService = reign.getService("messaging");

        // send message to a single node in the "service1" service in the "examples" cluster
        ResponseMessage responseMessage = messagingService.sendMessage("examples", "service1", "someNodeIdentifier",
                new SimpleRequestMessage("presence", "/"));

        // broadcast a message to all nodes belonging to the "service1" service in the examples cluster
        Map<String, ResponseMessage> responseMap = messagingService.sendMessage("examples", "service1",
                new SimpleRequestMessage("presence", "/examples"));

        /** coordination service example **/
        // get the coordination service
        CoordinationService coordService = (CoordinationService) reign.getService("coord");

        // get a distributed reentrant lock and use it
        DistributedReentrantLock lock = coordService.getReentrantLock("examples", "exclusive_lock1");
        lock.lock();
        try {
            // do some stuff here... (just sleeping 5 seconds)
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // do something here...

        } finally {
            lock.unlock();
            lock.destroy();
        }

        // get a read/write distributed lock and use it
        DistributedReadWriteLock rwLock = coordService.getReadWriteLock("examples", "rw_lock1");
        rwLock.readLock().lock();
        try {
            // do some stuff here... (just sleeping 5 seconds)
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // do something here...

        } finally {
            rwLock.readLock().unlock();
            rwLock.destroy();
        }

        /** sleep to allow examples to run for a bit **/
        Thread.sleep(120000);

        /** shutdown reign **/
        reign.stop();

        /** sleep a bit to observe observer callbacks **/
        Thread.sleep(10000);
    }
}
