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
        /** init and start reign using builder **/
        Reign reign = Reign.maker().zkClient("localhost:2181", 30000).pathCache(1024, 8).core().get();
        reign.start();

        /** presence service example **/
        presenceServiceExample(reign);

        /** sleep to allow examples to run for a bit **/
        Thread.sleep(600000);

        /** shutdown reign **/
        reign.stop();

        /** sleep a bit to observe observer callbacks **/
        Thread.sleep(10000);
    }

    public static void presenceServiceExample(Reign reign) throws Exception {
        // get presence service
        final PresenceService presenceService = reign.getService("presence");

        // separate thread to exercise waitUntilAvailable for ServiceInfo
        Thread t1 = new Thread() {
            @Override
            public void run() {
                ServiceInfo serviceInfo = presenceService.waitUntilAvailable("examples", "service1",
                        new SimplePresenceObserver<ServiceInfo>() {
                            @Override
                            public void updated(ServiceInfo info) {
                                if (info != null) {
                                    logger.info("***** T1:  Observer:  serviceInfo={}",
                                            ReflectionToStringBuilder.toString(info, ToStringStyle.DEFAULT_STYLE));
                                } else {
                                    logger.info("***** T1:  Observer:  serviceInfo deleted");
                                }
                            }
                        }, -1);
                logger.info("T1:  serviceInfo={}",
                        ReflectionToStringBuilder.toString(serviceInfo, ToStringStyle.DEFAULT_STYLE));
            }
        };
        t1.start();

        // separate thread to exercise waitUntilAvailable for NodeInfo
        Thread t2 = new Thread() {
            @Override
            public void run() {
                NodeInfo nodeInfo = presenceService.waitUntilAvailable("examples", "service1", "node1",
                        new SimplePresenceObserver<NodeInfo>() {
                            @Override
                            public void updated(NodeInfo info) {
                                if (info != null) {
                                    logger.info("***** T2:  Observer:  nodeInfo={}",
                                            ReflectionToStringBuilder.toString(info, ToStringStyle.DEFAULT_STYLE));
                                } else {
                                    logger.info("***** T2:  Observer:  nodeInfo deleted");
                                }
                            }
                        }, -1);
                logger.info("T2:  nodeInfo={}",
                        ReflectionToStringBuilder.toString(nodeInfo, ToStringStyle.DEFAULT_STYLE));
            }
        };
        t2.start();

        Thread t3 = new Thread() {
            @Override
            public void run() {
                NodeInfo nodeInfo = presenceService.waitUntilAvailable("examples", "service1", "node1",
                        new SimplePresenceObserver<NodeInfo>() {
                            @Override
                            public void updated(NodeInfo info) {
                                if (info != null) {
                                    logger.info("***** T3:  Observer:  nodeInfo={}",
                                            ReflectionToStringBuilder.toString(info, ToStringStyle.DEFAULT_STYLE));
                                } else {
                                    logger.info("***** T3:  Observer:  nodeInfo deleted");
                                }
                            }
                        }, 10000);
                logger.info("T3:  nodeInfo={}",
                        ReflectionToStringBuilder.toString(nodeInfo, ToStringStyle.DEFAULT_STYLE));
            }
        };
        t3.start();

        Thread.sleep(3000);

        // try to retrieve service info (which may not be immediately
        // available); include observer to be notified of changes in service
        // info
        ServiceInfo serviceInfo = presenceService.lookupServiceInfo("examples", "service1",
                new SimplePresenceObserver<ServiceInfo>() {
                    @Override
                    public void updated(ServiceInfo info) {
                        if (info != null) {
                            logger.info("***** Observer:  serviceInfo={}",
                                    ReflectionToStringBuilder.toString(info, ToStringStyle.DEFAULT_STYLE));
                        } else {
                            logger.info("***** Observer:  serviceInfo deleted");
                        }
                    }
                });
        logger.info("serviceInfo={}", ReflectionToStringBuilder.toString(serviceInfo, ToStringStyle.DEFAULT_STYLE));

        // try to retrieve node info (which may not be immediately
        // available); include observer to be notified of changes in node
        // info
        NodeInfo nodeInfo = presenceService.lookupNodeInfo("examples", "service2", "node1",
                new SimplePresenceObserver<NodeInfo>() {
                    @Override
                    public void updated(NodeInfo info) {
                        if (info != null) {
                            logger.info("***** Observer:  nodeInfo={}",
                                    ReflectionToStringBuilder.toString(info, ToStringStyle.DEFAULT_STYLE));

                        } else {
                            logger.info("***** Observer:  nodeInfo deleted");
                        }
                    }
                });
        logger.info("nodeInfo={}", ReflectionToStringBuilder.toString(nodeInfo, ToStringStyle.DEFAULT_STYLE));

        // basic service node announcement
        presenceService.announce("examples", "service1", true);

        // service node announcement with some additional info
        Map<String, String> nodeAttributes = new HashMap<String, String>();
        nodeAttributes.put("port", "1234");
        presenceService.announce("examples", "service2", true, nodeAttributes);

        // sleep a bit
        Thread.sleep(10000);

        presenceService.hide("examples", "service2");

        // sleep a bit
        Thread.sleep(10000);

        presenceService.show("examples", "service2");

        // new node available in service
        presenceService.announce("examples", "service1", true);

        // sleep a bit
        Thread.sleep(10000);

        // new node available in service
        presenceService.hide("examples", "service1");

        // reannounce service with changed attributes
        // service node announcement with some additional info
        nodeAttributes = new HashMap<String, String>();
        nodeAttributes.put("port", "9999");
        presenceService.announce("examples", "service2", true, nodeAttributes);

    }
}
