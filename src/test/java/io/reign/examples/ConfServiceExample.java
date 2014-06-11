/*
 * Copyright 2013 Yen Pai ypai@reign.io
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.reign.examples;

import io.reign.Reign;
import io.reign.conf.ConfObserver;
import io.reign.conf.ConfService;

import java.util.Map;
import java.util.Properties;

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
        /** init and start reign using builder **/
        final Reign reign = Reign.maker().zkClient("localhost:2181", 15000).pathCache(1024, 8).get();
        reign.start();

        /** conf service example **/
        Thread t1 = new Thread() {
            @Override
            public void run() {
                try {
                    confServiceExample(reign);
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
                    confServiceExample(reign);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        };
        t2.start();
        logger.info("DONE");

        /** sleep to allow examples to run for a bit **/
        Thread.sleep(600000);

        /** shutdown reign **/
        reign.stop();

        /** sleep a bit to observe observer callbacks **/
        Thread.sleep(10000);
    }

    public static void confServiceExample(Reign reign) throws Exception {

        // this is how you would normally get a service
        ConfService confService = (ConfService) reign.getService("conf");

        // load a configuration which will not be immediately available but pass
        // observer to be notified of changes in configuration
        Properties loadedConf = null;
        confService.observe("examples", "config1.properties", new ConfObserver<Map<String, Object>>() {
            @Override
            public void updated(Map<String, Object> conf, Map<String, Object> oldConf) {
                if (conf != null) {
                    logger.info("***** Observer:  conf={}; oldConf={}", conf, oldConf);

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
        confService.putConf("examples", "config1.properties", conf);

        Thread.sleep(10000);

        // change configuration
        conf = new Properties();
        conf.setProperty("capacity.min", "333");
        conf.setProperty("capacity.max", "1024");
        conf.setProperty("lastSavedTimestamp", System.currentTimeMillis() + "");
        confService.putConf("examples", "config1.properties", conf);

        Thread.sleep(10000);

        // retrieve again using convenience method
        loadedConf = confService.getConf("examples", "config1.properties");

        // remove configuration
        confService.removeConf("examples", "config1.properties");
    }
}
