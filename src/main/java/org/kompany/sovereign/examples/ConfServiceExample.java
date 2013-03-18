package org.kompany.sovereign.examples;

import java.util.Properties;

import org.kompany.sovereign.Sovereign;
import org.kompany.sovereign.conf.ConfObserver;
import org.kompany.sovereign.conf.ConfProperties;
import org.kompany.sovereign.conf.ConfPropertiesSerializer;
import org.kompany.sovereign.conf.ConfService;
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
        /** init and start sovereign using builder **/
        final Sovereign sovereign = Sovereign.builder().zkClient("localhost:2181", 15000).pathCache(1024, 8)
                .allCoreServices().build();
        // sovereign.start();

        /** conf service example **/
        Thread t1 = new Thread() {
            @Override
            public void run() {
                try {
                    confServiceExample(sovereign);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        };
        t1.start();
        Thread t2 = new Thread() {
            @Override
            public void run() {
                try {
                    confServiceExample(sovereign);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        };
        t2.start();
        logger.info("DONE");

        /** sleep to allow examples to run for a bit **/
        Thread.sleep(60000);

        /** shutdown sovereign **/
        sovereign.stop();

        /** sleep a bit to observe observer callbacks **/
        Thread.sleep(10000);
    }

    public static void confServiceExample(Sovereign sovereign) throws Exception {
        sovereign.start();

        // this is how you would normally get a service
        ConfService confService = (ConfService) sovereign.getService("conf");

        // load a configuration which will not be immediately available but pass
        // observer to be notified of changes in configuration
        Properties loadedConf = confService.getConf("examples-cluster", "config1.properties",
                new ConfPropertiesSerializer<ConfProperties>(false), new ConfObserver<ConfProperties>() {
                    @Override
                    public void updated(ConfProperties conf) {
                        if (conf != null) {
                            logger.info("***** Observer:  conf={}", conf);

                        } else {
                            logger.info("***** Observer:  conf deleted");
                        }
                    }
                });
        logger.info("loadedConf={}", loadedConf);

        // save the configuration that we were trying to access above; the
        // observer will be notified of the change
        Properties conf = new Properties();
        conf.setProperty("capacity.min", "111");
        conf.setProperty("capacity.max", "999");
        conf.setProperty("lastSavedTimestamp", System.currentTimeMillis() + "");
        confService.putConf("examples-cluster", "config1.properties", conf, new ConfPropertiesSerializer<Properties>(
                false));

        Thread.sleep(10000);

        // change configuration
        conf = new Properties();
        conf.setProperty("capacity.min", "333");
        conf.setProperty("capacity.max", "1024");
        conf.setProperty("lastSavedTimestamp", System.currentTimeMillis() + "");
        confService.putConf("examples-cluster", "config1.properties", conf, new ConfPropertiesSerializer<Properties>(
                false));

        Thread.sleep(10000);

        // remove configuration
        confService.removeConf("examples-cluster", "config1.properties");
    }
}
