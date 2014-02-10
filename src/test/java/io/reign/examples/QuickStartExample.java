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

package io.reign.examples;

import io.reign.CanonicalId;
import io.reign.Reign;
import io.reign.conf.ConfService;
import io.reign.coord.CoordinationService;
import io.reign.coord.DistributedReadWriteLock;
import io.reign.coord.DistributedReentrantLock;
import io.reign.mesg.DefaultMessagingService;
import io.reign.mesg.ResponseMessage;
import io.reign.mesg.SimpleRequestMessage;
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
        /**
         * init and start with core services -- connecting to ZooKeeper on localhost at port 2181 with 30 second
         * ZooKeeper session timeout
         **/
        Reign reign = Reign.maker().zkClient("localhost:2181", 30000).get();
        reign.start();

        /** init and start using Spring convenience builder **/
        // SpringReignMaker springReignMaker = new SpringReignMaker();
        // springReignMaker.setZkConnectString("localhost:2181");
        // springReignMaker.setZkSessionTimeout(30000);
        // springReignMaker.setCore(true);
        // springReignMaker.initStart();
        // Reign reign = springReignMaker.get();

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

        // show service1 again
        presenceService.show("examples", "service1");

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
        DefaultMessagingService messagingService = reign.getService("messaging");

        // wait indefinitely for at least one node in "service1" to become available
        presenceService.waitUntilAvailable("examples", "service1", -1);

        // send message to a single node in the "service1" service in the "examples" cluster;
        // in this example, we are just messaging ourselves
        CanonicalId canonicalId = reign.getCanonicalId();
        String canonicalIdString = reign.getPathScheme().toPathToken(canonicalId);
        ResponseMessage responseMessage = messagingService.sendMessage("examples", "service1", canonicalIdString,
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

            // don't have to do this if re-using this lock object
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

            // don't have to do this if re-using this lock object
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
