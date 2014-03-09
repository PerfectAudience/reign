package io.reign.examples;

import io.reign.Reign;
import io.reign.metrics.MetricsService;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

public class MetricsServiceExample {
    private static final Logger logger = LoggerFactory.getLogger(MetricsServiceExample.class);

    public static void main(String[] args) throws Exception {
        /** init and start reign using builder **/
        Reign reign = Reign.maker().zkClient("localhost:2181", 30000).pathCache(1024, 8).get();
        reign.start();

        MetricsService metricsService = reign.getService("metrics");

        MetricRegistry registry = new MetricRegistry();
        Counter counter1 = registry.counter(MetricRegistry.name(MetricsService.class, "counter1"));
        Counter counter2 = registry.counter(MetricRegistry.name(MetricsService.class, "counter2"));
        counter1.inc();
        counter2.inc(3);

        metricsService.exportMetrics("clusterA", "serviceA", registry, 10, TimeUnit.SECONDS);

        Thread.sleep(5000);
        Counter counter3 = registry.counter(MetricRegistry.name(MetricsService.class, "counter3"));
        counter3.inc(5);

        Thread.sleep(60000);
    }
}
