package org.kompany.overlord.examples;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.kompany.overlord.Sovereign;
import org.kompany.overlord.SovereignBuilder;
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
        /** init and start sovereign using builder **/
        Sovereign sovereign = (new SovereignBuilder()).zkConfig("localhost:2181", 15000).pathCache(1024, 8)
                .allCoreServices().build();
        sovereign.start();

        /** presence service example **/
        presenceServiceExample(sovereign);

        /** sleep to allow examples to run for a bit **/
        Thread.sleep(60000);

        /** shutdown sovereign **/
        sovereign.stop();

        /** sleep a bit to observe observer callbacks **/
        Thread.sleep(10000);
    }

    public static void presenceServiceExample(Sovereign sovereign) throws Exception {
        // get presence service
        PresenceService presenceService = (PresenceService) sovereign.getService("presence");

        // try to retrieve service info (which may not be immediately
        // available); include observer to be notified of changes in service
        // info
        ServiceInfo serviceInfo = presenceService.lookup("examples-cluster", "service1",
                new PresenceObserver<ServiceInfo>() {
                    @Override
                    public void updated(ServiceInfo info) {
                        if (info != null) {
                            logger.info("Observer:  serviceInfo={}",
                                    ReflectionToStringBuilder.toString(info, ToStringStyle.DEFAULT_STYLE));
                        } else {
                            logger.info("Observer:  serviceInfo deleted");
                        }
                    }
                });
        logger.info("serviceInfo={}", ReflectionToStringBuilder.toString(serviceInfo, ToStringStyle.DEFAULT_STYLE));

        // try to retrieve node info (which may not be immediately
        // available); include observer to be notified of changes in node
        // info
        NodeInfo nodeInfo = presenceService.lookup("examples-cluster", "service2", "node1",
                new PresenceObserver<NodeInfo>() {
                    @Override
                    public void updated(NodeInfo info) {
                        if (info != null) {
                            logger.info("Observer:  nodeInfo={}",
                                    ReflectionToStringBuilder.toString(info, ToStringStyle.DEFAULT_STYLE));

                        } else {
                            logger.info("Observer:  nodeInfo deleted");
                        }
                    }
                });
        logger.info("nodeInfo={}", ReflectionToStringBuilder.toString(nodeInfo, ToStringStyle.DEFAULT_STYLE));

        // basic service node announcement
        presenceService.announce("examples-cluster", "service1", "node1");

        // service node announcement with some additional info
        Map<String, String> nodeAttributes = new HashMap<String, String>();
        nodeAttributes.put("port", "1234");
        presenceService.announce("examples-cluster", "service2", "node1", nodeAttributes);

        // sleep a bit
        Thread.sleep(10000);

        presenceService.hide("examples-cluster", "service2", "node1");

        // sleep a bit
        Thread.sleep(10000);

        presenceService.unhide("examples-cluster", "service2", "node1");

        // new node available in service
        presenceService.announce("examples-cluster", "service1", "node2");

        // sleep a bit
        Thread.sleep(10000);

        // new node available in service
        presenceService.hide("examples-cluster", "service1", "node2");

        // reannounce service with changed attributes
        // service node announcement with some additional info
        nodeAttributes = new HashMap<String, String>();
        nodeAttributes.put("port", "9999");
        presenceService.announce("examples-cluster", "service2", "node1", nodeAttributes);

    }
}
