package io.reign.examples;

import io.reign.Reign;
import io.reign.metrics.MetricsData;
import io.reign.metrics.MetricsService;
import io.reign.metrics.RotatingMetricRegistryManager;
import io.reign.presence.PresenceService;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

public class MetricsServiceExample {
    private static final Logger logger = LoggerFactory.getLogger(MetricsServiceExample.class);

    public static void main(String[] args) throws Exception {
        // logger.info("MetricsData JSON = {}", JacksonUtil.getObjectMapper().writeValueAsString(new MetricsData()));

        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);

        /** init and start reign using builder **/
        Reign reign = Reign.maker().zkClient("localhost:2181", 30000).pathCache(1024, 8).get();
        reign.start();

        PresenceService presenceService = reign.getService("presence");
        presenceService.announce("clusterA", "serviceA", true);

        MetricsService metricsService = reign.getService("metrics");

        final RotatingMetricRegistryManager registryManager = new RotatingMetricRegistryManager(120, TimeUnit.SECONDS);
        Counter counter1 = registryManager.get().counter(MetricRegistry.name("counter1"));
        Counter counter2 = registryManager.get().counter(MetricRegistry.name("counter2"));
        counter1.inc();
        counter2.inc(3);

        metricsService.scheduleExport("clusterA", "serviceA", registryManager, 2, TimeUnit.SECONDS);

        MetricsData metricsData = null;
        while ((metricsData = metricsService.getMyMetrics("clusterA", "serviceA")) == null) {
            Thread.sleep(1000);
        }

        logger.debug("counter1={}", metricsData.getCounter("counter1").getCount());

        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                Counter c1 = registryManager.get().counter(MetricRegistry.name("c1"));
                Counter c2 = registryManager.get().counter(MetricRegistry.name("c2"));
                c1.inc();
                c2.inc(3);

            }
        }, 0, 1, TimeUnit.SECONDS);

        Thread.sleep(35000);
        Counter counter3 = registryManager.get().counter(MetricRegistry.name(MetricsService.class, "counter3"));
        counter3.inc(5);

        Thread.sleep(600000);
    }
}
