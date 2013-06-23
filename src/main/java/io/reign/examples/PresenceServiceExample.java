package io.reign.examples;

import io.reign.Reign;
import io.reign.presence.NodeInfo;
import io.reign.presence.PresenceService;
import io.reign.presence.ServiceInfo;
import io.reign.presence.SimplePresenceObserver;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
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
        Reign sovereign = Reign.builder().zkClient("localhost:2181", 15000).pathCache(1024, 8)
                .allCoreServices().build();
        sovereign.start();

        /** presence service example **/
        presenceServiceExample(sovereign);

        /** sleep to allow examples to run for a bit **/
        Thread.sleep(120000);

        /** shutdown sovereign **/
        sovereign.stop();

        /** sleep a bit to observe observer callbacks **/
        Thread.sleep(10000);
    }

    public static void presenceServiceExample(Reign sovereign) throws Exception {
        // get presence service
        final PresenceService presenceService = sovereign.getService("presence");

        // separate thread to exercise waitUntilAvailable for ServiceInfo
        Thread t1 = new Thread() {
            @Override
            public void run() {
                ServiceInfo serviceInfo = presenceService.waitUntilAvailable("examples-cluster", "service1",
                        new SimplePresenceObserver<ServiceInfo>() {
                            @Override
                            public void updated(ServiceInfo info) {
                                if (info != null) {
                                    logger.info("***** T1:  Observer:  serviceInfo={}", ReflectionToStringBuilder
                                            .toString(info, ToStringStyle.DEFAULT_STYLE));
                                } else {
                                    logger.info("***** T1:  Observer:  serviceInfo deleted");
                                }
                            }
                        }, -1);
                logger.info("T1:  serviceInfo={}", ReflectionToStringBuilder.toString(serviceInfo,
                        ToStringStyle.DEFAULT_STYLE));
            }
        };
        t1.start();

        // separate thread to exercise waitUntilAvailable for NodeInfo
        Thread t2 = new Thread() {
            @Override
            public void run() {
                NodeInfo nodeInfo = presenceService.waitUntilAvailable("examples-cluster", "service1", "node1",
                        new SimplePresenceObserver<NodeInfo>() {
                            @Override
                            public void updated(NodeInfo info) {
                                if (info != null) {
                                    logger.info("***** T2:  Observer:  nodeInfo={}", ReflectionToStringBuilder
                                            .toString(info, ToStringStyle.DEFAULT_STYLE));
                                } else {
                                    logger.info("***** T2:  Observer:  nodeInfo deleted");
                                }
                            }
                        }, -1);
                logger.info("T2:  nodeInfo={}", ReflectionToStringBuilder.toString(nodeInfo,
                        ToStringStyle.DEFAULT_STYLE));
            }
        };
        t2.start();

        Thread t3 = new Thread() {
            @Override
            public void run() {
                NodeInfo nodeInfo = presenceService.waitUntilAvailable("examples-cluster", "service1", "node1",
                        new SimplePresenceObserver<NodeInfo>() {
                            @Override
                            public void updated(NodeInfo info) {
                                if (info != null) {
                                    logger.info("***** T3:  Observer:  nodeInfo={}", ReflectionToStringBuilder
                                            .toString(info, ToStringStyle.DEFAULT_STYLE));
                                } else {
                                    logger.info("***** T3:  Observer:  nodeInfo deleted");
                                }
                            }
                        }, 10000);
                logger.info("T3:  nodeInfo={}", ReflectionToStringBuilder.toString(nodeInfo,
                        ToStringStyle.DEFAULT_STYLE));
            }
        };
        t3.start();

        Thread.sleep(3000);

        // try to retrieve service info (which may not be immediately
        // available); include observer to be notified of changes in service
        // info
        ServiceInfo serviceInfo = presenceService.lookupServiceInfo("examples-cluster", "service1",
                new SimplePresenceObserver<ServiceInfo>() {
                    @Override
                    public void updated(ServiceInfo info) {
                        if (info != null) {
                            logger.info("***** Observer:  serviceInfo={}", ReflectionToStringBuilder.toString(info,
                                    ToStringStyle.DEFAULT_STYLE));
                        } else {
                            logger.info("***** Observer:  serviceInfo deleted");
                        }
                    }
                });
        logger.info("serviceInfo={}", ReflectionToStringBuilder.toString(serviceInfo, ToStringStyle.DEFAULT_STYLE));

        // try to retrieve node info (which may not be immediately
        // available); include observer to be notified of changes in node
        // info
        NodeInfo nodeInfo = presenceService.lookupNodeInfo("examples-cluster", "service2", "node1",
                new SimplePresenceObserver<NodeInfo>() {
                    @Override
                    public void updated(NodeInfo info) {
                        if (info != null) {
                            logger.info("***** Observer:  nodeInfo={}", ReflectionToStringBuilder.toString(info,
                                    ToStringStyle.DEFAULT_STYLE));

                        } else {
                            logger.info("***** Observer:  nodeInfo deleted");
                        }
                    }
                });
        logger.info("nodeInfo={}", ReflectionToStringBuilder.toString(nodeInfo, ToStringStyle.DEFAULT_STYLE));

        // basic service node announcement
        presenceService.announce("examples-cluster", "service1", sovereign.getPathScheme().getCanonicalId(4731), true);

        // service node announcement with some additional info
        Map<String, String> nodeAttributes = new HashMap<String, String>();
        nodeAttributes.put("port", "1234");
        presenceService.announce("examples-cluster", "service2", "node1", true, nodeAttributes);

        // sleep a bit
        Thread.sleep(10000);

        presenceService.hide("examples-cluster", "service2", "node1");

        // sleep a bit
        Thread.sleep(10000);

        presenceService.unhide("examples-cluster", "service2", "node1");

        // new node available in service
        presenceService.announce("examples-cluster", "service1", "node2", true);

        // sleep a bit
        Thread.sleep(10000);

        // new node available in service
        presenceService.hide("examples-cluster", "service1", "node2");

        // reannounce service with changed attributes
        // service node announcement with some additional info
        nodeAttributes = new HashMap<String, String>();
        nodeAttributes.put("port", "9999");
        presenceService.announce("examples-cluster", "service2", "node1", true, nodeAttributes);

    }
}
