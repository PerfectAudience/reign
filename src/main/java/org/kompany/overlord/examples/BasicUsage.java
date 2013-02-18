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

        /** use presence service to announce that service nodes are up **/
        // this is how you would normally get a service
        presenceService = (PresenceService) sovereign.getService("presence");

        // basic node announcement
        presenceService.announce("examples", "test-service1", "node1");

        // node with some additional info
        Map<String, Object> nodeAttributes = new HashMap<String, Object>();
        nodeAttributes.put("port", 1234);
        presenceService.announce("examples", "test-service2", "node1", nodeAttributes);

        Thread.sleep(10000);

        sovereign.stop();

        System.out.println("***** END *****");
    }
}
