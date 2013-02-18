package org.kompany.overlord.examples;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.kompany.overlord.Service;
import org.kompany.overlord.Sovereign;
import org.kompany.overlord.conf.ConfService;
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

        /** use presence service to announce that service nodes are up **/
        // this is how you would normally get a service
        confService = (ConfService) sovereign.getService("conf");

        // save a configuration
        Properties conf = new Properties();
        conf.setProperty("capacity.min", "111");
        conf.setProperty("capacity.max", "999");
        conf.setProperty("lastSavedTimestamp", System.currentTimeMillis() + "");
        confService.putConf("examples/config1", conf, new PropertiesConfSerializer(false));

        // load a configuration
        Properties loadedConf = confService.getConf("examples/config1", new PropertiesConfSerializer(false));
        logger.debug("loadedConf={}", loadedConf);

        // sleep to allow initialization and announcements to happen
        Thread.sleep(20000);

        sovereign.stop();

    }
}
