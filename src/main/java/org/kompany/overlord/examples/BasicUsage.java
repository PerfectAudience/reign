package org.kompany.overlord.examples;

import java.util.HashMap;
import java.util.Map;

import org.kompany.overlord.Service;
import org.kompany.overlord.Sovereign;
import org.kompany.overlord.presence.PresenceService;

/**
 * Demonstrate basic usage.
 * 
 * @author ypai
 * 
 */
public class BasicUsage {
    public static void main(String[] args) throws Exception {
        /** init sovereign using default ZkClient implementation **/
        Sovereign sovereign = new Sovereign("localhost:2181", 15000);

        /** set up zk client if you want to use some other implementation **/
        // ZkClient zkClient = new ResilientZooKeeper("localhost:2181", 15000);
        // sovereign.setZkClient(zkClient);

        /** set-up services and register **/
        Map<String, Service> serviceMap = new HashMap<String, Service>();

        // presence service
        PresenceService presenceService = new PresenceService();
        serviceMap.put("presence", presenceService);
        sovereign.registerServices(serviceMap);

        /** start sovereign **/
        sovereign.start();

        presenceService.announce("examples", "test-service1", "node1");
        presenceService.announce("examples", "test-service2", "node1");

        Thread.sleep(5000);

    }
}
