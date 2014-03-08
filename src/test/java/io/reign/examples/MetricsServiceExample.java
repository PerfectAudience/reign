package io.reign.examples;

import io.reign.Reign;
import io.reign.metrics.MetricsService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsServiceExample {
    private static final Logger logger = LoggerFactory.getLogger(MetricsServiceExample.class);

    public static void main(String[] args) throws Exception {
        /** init and start reign using builder **/
        Reign reign = Reign.maker().zkClient("localhost:2181", 30000).pathCache(1024, 8).get();
        reign.start();

        MetricsService metricsService = reign.getService("metrics");

    }
}
