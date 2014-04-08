package io.reign.metrics;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import io.reign.util.JacksonUtil;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkMetricsReporterTest {
    private static final Logger logger = LoggerFactory.getLogger(ZkMetricsReporterTest.class);

    @Test
    public void testNoMetrics() throws Exception {
        try {
            RotatingMetricRegistryManager registryManager = new RotatingMetricRegistryManager(300, TimeUnit.SECONDS);

            final ZkMetricsReporter reporter = ZkMetricsReporter.forRegistry(registryManager)
                    .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build();

            String string = reporter.report(new StringBuilder()).toString();
            logger.debug(string);

            byte[] bytes = string.getBytes("UTF-8");
            JacksonUtil.getObjectMapper().readValue(bytes, MetricsData.class);
            assertTrue(true);
        } catch (Exception e) {
            logger.error("" + e, e);
            assertFalse(true);
        }
    }
}
