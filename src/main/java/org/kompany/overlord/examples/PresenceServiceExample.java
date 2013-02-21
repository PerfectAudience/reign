package org.kompany.overlord.examples;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.kompany.overlord.Service;
import org.kompany.overlord.Sovereign;
import org.kompany.overlord.presence.NodeInfo;
import org.kompany.overlord.presence.PresenceObserver;
import org.kompany.overlord.presence.PresenceService;
import org.kompany.overlord.presence.ServiceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Demonstrate basic usage.
 * 
 * @author ypai
 * 
 */
public class PresenceServiceExample {

    private static final Logger logger = LoggerFactory.getLogger(PresenceServiceExample.class);

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

        // basic service node announcement
        presenceService.announce("examples", "service1", "node1");

        // service node accouncement with some additional info
        Map<String, String> nodeAttributes = new HashMap<String, String>();
        nodeAttributes.put("port", "1234");
        presenceService.announce("examples", "service2", "node1", nodeAttributes);

        /** look up service and node info **/
        // service info
        PresenceObserver<ServiceInfo> serviceObserver = new PresenceObserver<ServiceInfo>() {
            @Override
            public void handle(ServiceInfo info) {
                if (info != null) {
                    logger.info("Observer:  serviceInfo={}",
                            ReflectionToStringBuilder.toString(info, ToStringStyle.DEFAULT_STYLE));
                } else {
                    logger.info("Observer:  serviceInfo deleted");
                }
            }
        };
        ServiceInfo serviceInfo = presenceService.lookup("examples", "service1", serviceObserver);
        logger.info("serviceInfo={}", ReflectionToStringBuilder.toString(serviceInfo, ToStringStyle.DEFAULT_STYLE));

        // node info
        PresenceObserver<NodeInfo> nodeObserver = new PresenceObserver<NodeInfo>() {
            @Override
            public void handle(NodeInfo info) {
                if (info != null) {
                    logger.info("Observer:  nodeInfo={}",
                            ReflectionToStringBuilder.toString(info, ToStringStyle.DEFAULT_STYLE));

                } else {
                    logger.info("Observer:  nodeInfo deleted");
                }
            }
        };
        NodeInfo nodeInfo = presenceService.lookup("examples", "service2", "node1", nodeObserver);
        logger.info("nodeInfo={}", ReflectionToStringBuilder.toString(nodeInfo, ToStringStyle.DEFAULT_STYLE));

        // sleep to allow initialization and announcements to happen
        Thread.sleep(60000);

        sovereign.stop();

    }
}
