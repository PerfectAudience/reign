package io.reign.examples;

import io.reign.Reign;
import io.reign.metrics.MetricsData;
import io.reign.metrics.MetricsService;
import io.reign.metrics.RotatingMetricRegistryRef;
import io.reign.presence.PresenceService;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

public class MetricsServiceExample {
    private static final Logger logger = LoggerFactory.getLogger(MetricsServiceExample.class);

    public static void main(String[] args) throws Exception {
        // logger.info("MetricsData JSON = {}", JacksonUtil.getObjectMapper().writeValueAsString(new MetricsData()));

        /** init and start reign using builder **/
        Reign reign = Reign.maker().zkClient("localhost:2181", 30000).pathCache(1024, 8).get();
        reign.start();

        PresenceService presenceService = reign.getService("presence");
        presenceService.announce("clusterA", "serviceA", true);

        MetricsService metricsService = reign.getService("metrics");

        RotatingMetricRegistryRef registryRef = new RotatingMetricRegistryRef(30, TimeUnit.SECONDS);
        Counter counter1 = registryRef.get().counter(MetricRegistry.name("counter1"));
        Counter counter2 = registryRef.get().counter(MetricRegistry.name("counter2"));
        counter1.inc();
        counter2.inc(3);

        // logger.info("MetricsData JSON = {}", JacksonUtil.getObjectMapper().writeValueAsString(new MetricsData()));
        // MetricsData metricsDataTest = JacksonUtil
        // .getObjectMapper()
        // .readValue(
        // "{\"interval_start_ts\":1394385681765,\"interval_length\":30,\"interval_length_unit\":\"SECONDS\",\"counters\":{\"counter1\":{\"count\":\"1\"},\"counter2\":{\"count\":\"3\"}}}",
        // MetricsData.class);
        // logger.info("MetricsData Test JSON = {}", JacksonUtil.getObjectMapper().writeValueAsString(metricsDataTest));

        metricsService.exportMetrics("clusterA", "serviceA", registryRef, 10, TimeUnit.SECONDS);

        MetricsData metricsData = null;
        while ((metricsData = metricsService.getMetrics("clusterA", "serviceA")) == null) {
            Thread.sleep(1000);
        }

        logger.debug("counter1={}", metricsData.getCounter("counter1").getCount());

        Thread.sleep(35000);
        Counter counter3 = registryRef.get().counter(MetricRegistry.name(MetricsService.class, "counter3"));
        counter3.inc(5);

        Thread.sleep(60000);
    }
}
