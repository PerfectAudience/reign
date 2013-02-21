package org.kompany.overlord.examples;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.kompany.overlord.Sovereign;
import org.kompany.overlord.SovereignBuilder;
import org.kompany.overlord.conf.ConfObserver;
import org.kompany.overlord.conf.ConfService;
import org.kompany.overlord.conf.PropertiesConf;
import org.kompany.overlord.conf.PropertiesConfSerializer;
import org.kompany.overlord.presence.NodeInfo;
import org.kompany.overlord.presence.PresenceObserver;
import org.kompany.overlord.presence.PresenceService;
import org.kompany.overlord.presence.ServiceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Demonstrates basic usage of Sovereign's core services.
 * 
 * @author ypai
 * 
 */
public class QuickStartExample {

    private static final Logger logger = LoggerFactory.getLogger(QuickStartExample.class);

    public static void main(String[] args) throws Exception {
        /** init and start sovereign using builder **/
        Sovereign sovereign = (new SovereignBuilder()).zkConfig("localhost:2181", 15000).pathCache(1024, 8)
                .allCoreServices().build();
        sovereign.start();

        /** presence service example **/
        presenceServiceExample(sovereign);

        /** conf service example **/
        // confServiceExample(sovereign);

        /** sleep to allow examples to run for a bit **/
        Thread.sleep(60000);

        /** shutdown sovereign **/
        sovereign.stop();

        /** sleep a bit to observe observer callbacks **/
        Thread.sleep(10000);
    }

    public static void confServiceExample(Sovereign sovereign) throws Exception {
        // this is how you would normally get a service
        ConfService confService = (ConfService) sovereign.getService("conf");

        // load a configuration which will not be immediately available but pass
        // observer to be notified of changes in configuration
        Properties loadedConf = confService.getConf("examples/config1.properties",
                new PropertiesConfSerializer<PropertiesConf>(false), new ConfObserver<PropertiesConf>() {
                    @Override
                    public void handle(PropertiesConf conf) {
                        if (conf != null) {
                            logger.info("Observer:  conf={}", conf);

                        } else {
                            logger.info("Observer:  conf deleted");
                        }
                    }
                });
        logger.debug("loadedConf={}", loadedConf);

        // save the configuration that we were trying to access above; the
        // observer will be notified of the change
        Properties conf = new Properties();
        conf.setProperty("capacity.min", "111");
        conf.setProperty("capacity.max", "999");
        conf.setProperty("lastSavedTimestamp", System.currentTimeMillis() + "");
        confService.putConf("examples/config1.properties", conf, new PropertiesConfSerializer<Properties>(false));

        Thread.sleep(10000);

        // change configuration
        conf = new Properties();
        conf.setProperty("capacity.min", "333");
        conf.setProperty("capacity.max", "1024");
        conf.setProperty("lastSavedTimestamp", System.currentTimeMillis() + "");
        confService.putConf("examples/config1.properties", conf, new PropertiesConfSerializer<Properties>(false));

        Thread.sleep(10000);

        // remove configuration
        confService.removeConf("examples/config1.properties");
    }

    public static void presenceServiceExample(Sovereign sovereign) throws Exception {
        // get presence service
        PresenceService presenceService = (PresenceService) sovereign.getService("presence");

        // try to retrieve service info (which may not be immediately
        // available); include observer to be notified of changes in service
        // info
        ServiceInfo serviceInfo = presenceService.lookup("examples", "service1", new PresenceObserver<ServiceInfo>() {
            @Override
            public void handle(ServiceInfo info) {
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
        NodeInfo nodeInfo = presenceService.lookup("examples", "service2", "node1", new PresenceObserver<NodeInfo>() {
            @Override
            public void handle(NodeInfo info) {
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
        presenceService.announce("examples", "service1", "node1");

        // service node announcement with some additional info
        Map<String, String> nodeAttributes = new HashMap<String, String>();
        nodeAttributes.put("port", "1234");
        presenceService.announce("examples", "service2", "node1", nodeAttributes);

        // sleep a bit
        Thread.sleep(10000);

        presenceService.hide("examples", "service2", "node1");

        // sleep a bit
        Thread.sleep(10000);

        presenceService.unhide("examples", "service2", "node1");

        // new node available in service
        presenceService.announce("examples", "service1", "node2");

        // sleep a bit
        Thread.sleep(10000);

        // new node available in service
        presenceService.hide("examples", "service1", "node2");

        // reannounce service with changed attributes
        // service node announcement with some additional info
        nodeAttributes = new HashMap<String, String>();
        nodeAttributes.put("port", "9999");
        presenceService.announce("examples", "service2", "node1", nodeAttributes);

    }
}
