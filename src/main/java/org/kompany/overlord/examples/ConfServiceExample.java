package org.kompany.overlord.examples;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.kompany.overlord.Service;
import org.kompany.overlord.Sovereign;
import org.kompany.overlord.conf.ConfObserver;
import org.kompany.overlord.conf.ConfService;
import org.kompany.overlord.conf.PropertiesConf;
import org.kompany.overlord.conf.PropertiesConfSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Demonstrate basic usage.
 * 
 * @author ypai
 * 
 */
public class ConfServiceExample {

    private static final Logger logger = LoggerFactory.getLogger(ConfServiceExample.class);

    public static void main(String[] args) throws Exception {
        /** init sovereign using default ZkClient implementation **/
        Sovereign sovereign = new Sovereign("localhost:2181", 15000);

        /** set up zk client if you want to use some other implementation **/
        // ZkClient zkClient = new ResilientZooKeeper("localhost:2181", 15000);
        // sovereign.setZkClient(zkClient);

        /** set-up services and register **/
        Map<String, Service> serviceMap = new HashMap<String, Service>();

        // conf service
        ConfService confService = new ConfService();
        serviceMap.put("conf", confService);
        sovereign.registerServices(serviceMap);

        /** start sovereign **/
        sovereign.start();

        /** use conf service to create a sample configuration **/
        // this is how you would normally get a service
        confService = (ConfService) sovereign.getService("conf");

        // load a configuration with observer
        ConfObserver<PropertiesConf> confObserver = new ConfObserver<PropertiesConf>() {
            @Override
            public void handle(PropertiesConf conf) {
                if (conf != null) {
                    logger.info("Observer:  conf={}", conf);

                } else {
                    logger.info("Observer:  conf deleted");
                }
            }
        };
        Properties loadedConf = confService.getConf("examples/config1.properties",
                new PropertiesConfSerializer<PropertiesConf>(false), confObserver);
        logger.debug("loadedConf={}", loadedConf);

        // save a configuration
        Properties conf = new Properties();
        conf.setProperty("capacity.min", "111");
        conf.setProperty("capacity.max", "999");
        conf.setProperty("lastSavedTimestamp", System.currentTimeMillis() + "");
        confService.putConf("examples/config1.properties", conf, new PropertiesConfSerializer<Properties>(false));

        // sleep
        Thread.sleep(20000);

        sovereign.stop();

    }
}
