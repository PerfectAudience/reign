package io.reign.metrics;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import io.reign.util.JacksonUtil;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;

public class ZkMetricsReporterTest {
    private static final Logger logger = LoggerFactory.getLogger(ZkMetricsReporterTest.class);

    @Test
    public void testNoMetrics() throws Exception {
        try {
            StaticMetricRegistryManager registryManager = new StaticMetricRegistryManager();

            final ZkMetricsReporter reporter = ZkMetricsReporter.builder().convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS).build();

            String string = reporter.report(registryManager.get(), registryManager.getLastRotatedTimestamp(),
                    registryManager.getRotationInterval(), registryManager.getRotationTimeUnit(), new StringBuilder())
                    .toString();
            logger.debug(string);

            byte[] bytes = string.getBytes("UTF-8");
            MetricsData metricsData = JacksonUtil.getObjectMapper().readValue(bytes, MetricsData.class);
            assertTrue(metricsData.getCounters().size() == 0);
        } catch (Exception e) {
            logger.error("" + e, e);
            assertFalse(true);
        }
    }

    @Test
    public void testSingleCounter() throws Exception {
        try {
            StaticMetricRegistryManager registryManager = new StaticMetricRegistryManager();
            Counter counter = registryManager.get().counter("test");
            counter.inc();

            final ZkMetricsReporter reporter = ZkMetricsReporter.builder().convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS).build();

            String string = reporter.report(registryManager.get(), registryManager.getLastRotatedTimestamp(),
                    registryManager.getRotationInterval(), registryManager.getRotationTimeUnit(), new StringBuilder())
                    .toString();
            logger.debug(string);

            byte[] bytes = string.getBytes("UTF-8");
            MetricsData metricsData = JacksonUtil.getObjectMapper().readValue(bytes, MetricsData.class);
            assertTrue(metricsData.getCounters().size() == 1);
            assertTrue(metricsData.getCounter("test").getCount() == 1);
        } catch (Exception e) {
            logger.error("" + e, e);
            assertFalse(true);
        }
    }

    @Test
    public void testSingleMeter() throws Exception {
        try {
            StaticMetricRegistryManager registryManager = new StaticMetricRegistryManager();
            Meter meter = registryManager.get().meter("test");
            meter.mark(1);

            final ZkMetricsReporter reporter = ZkMetricsReporter.builder().convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS).build();

            String string = reporter.report(registryManager.get(), registryManager.getLastRotatedTimestamp(),
                    registryManager.getRotationInterval(), registryManager.getRotationTimeUnit(), new StringBuilder())
                    .toString();
            logger.debug(string);

            byte[] bytes = string.getBytes("UTF-8");
            MetricsData metricsData = JacksonUtil.getObjectMapper().readValue(bytes, MetricsData.class);
            assertTrue(metricsData.getMeters().size() == 1);
            assertTrue(metricsData.getMeter("test").getCount() == 1);
        } catch (Exception e) {
            logger.error("" + e, e);
            assertFalse(true);
        }
    }

    @Test
    public void testSingleGauge() throws Exception {
        try {
            StaticMetricRegistryManager registryManager = new StaticMetricRegistryManager();
            Gauge<Integer> gauge = registryManager.get().register("test", new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                    return 1;
                }
            });

            final ZkMetricsReporter reporter = ZkMetricsReporter.builder().convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS).build();

            String string = reporter.report(registryManager.get(), registryManager.getLastRotatedTimestamp(),
                    registryManager.getRotationInterval(), registryManager.getRotationTimeUnit(), new StringBuilder())
                    .toString();
            logger.debug(string);

            byte[] bytes = string.getBytes("UTF-8");
            MetricsData metricsData = JacksonUtil.getObjectMapper().readValue(bytes, MetricsData.class);
            assertTrue(metricsData.getGauges().size() == 1);
            assertTrue(metricsData.getGauge("test").getValue() == 1);
        } catch (Exception e) {
            logger.error("" + e, e);
            assertFalse(true);
        }
    }

    @Test
    public void testSingleHistogram() throws Exception {
        try {
            StaticMetricRegistryManager registryManager = new StaticMetricRegistryManager();
            Histogram histogram = registryManager.get().histogram("test");
            histogram.update(1);

            final ZkMetricsReporter reporter = ZkMetricsReporter.builder().convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS).build();

            String string = reporter.report(registryManager.get(), registryManager.getLastRotatedTimestamp(),
                    registryManager.getRotationInterval(), registryManager.getRotationTimeUnit(), new StringBuilder())
                    .toString();
            logger.debug(string);

            byte[] bytes = string.getBytes("UTF-8");
            MetricsData metricsData = JacksonUtil.getObjectMapper().readValue(bytes, MetricsData.class);
            assertTrue(metricsData.getHistograms().size() == 1);
        } catch (Exception e) {
            logger.error("" + e, e);
            assertFalse(true);
        }
    }

    @Test
    public void testSingleTimer() throws Exception {
        try {
            StaticMetricRegistryManager registryManager = new StaticMetricRegistryManager();
            Timer timer = registryManager.get().timer("test");
            Context timerContext = timer.time();
            timerContext.stop();

            final ZkMetricsReporter reporter = ZkMetricsReporter.builder().convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS).build();

            String string = reporter.report(registryManager.get(), registryManager.getLastRotatedTimestamp(),
                    registryManager.getRotationInterval(), registryManager.getRotationTimeUnit(), new StringBuilder())
                    .toString();
            logger.debug(string);

            byte[] bytes = string.getBytes("UTF-8");
            MetricsData metricsData = JacksonUtil.getObjectMapper().readValue(bytes, MetricsData.class);
            assertTrue(metricsData.getTimers().size() == 1);
        } catch (Exception e) {
            logger.error("" + e, e);
            assertFalse(true);
        }
    }

    @Test
    public void testSingleCounterSingleTimer() throws Exception {
        try {
            StaticMetricRegistryManager registryManager = new StaticMetricRegistryManager();
            Counter counter = registryManager.get().counter("testCounter");
            counter.inc();
            Timer timer = registryManager.get().timer("testTimer");
            Context timerContext = timer.time();
            timerContext.stop();

            final ZkMetricsReporter reporter = ZkMetricsReporter.builder().convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS).build();

            String string = reporter.report(registryManager.get(), registryManager.getLastRotatedTimestamp(),
                    registryManager.getRotationInterval(), registryManager.getRotationTimeUnit(), new StringBuilder())
                    .toString();
            logger.debug(string);

            byte[] bytes = string.getBytes("UTF-8");
            MetricsData metricsData = JacksonUtil.getObjectMapper().readValue(bytes, MetricsData.class);
            assertTrue(metricsData.getCounters().size() == 1);
            assertTrue(metricsData.getCounter("testCounter").getCount() == 1);
            assertTrue(metricsData.getTimers().size() == 1);
        } catch (Exception e) {
            logger.error("" + e, e);
            assertFalse(true);
        }
    }

    @Test
    public void testSingleGaugeSingleMeter() throws Exception {
        try {
            StaticMetricRegistryManager registryManager = new StaticMetricRegistryManager();
            Gauge<Integer> gauge = registryManager.get().register("testGauge", new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                    return 1;
                }
            });
            Meter meter = registryManager.get().meter("testMeter");
            meter.mark(1);

            final ZkMetricsReporter reporter = ZkMetricsReporter.builder().convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS).build();

            String string = reporter.report(registryManager.get(), registryManager.getLastRotatedTimestamp(),
                    registryManager.getRotationInterval(), registryManager.getRotationTimeUnit(), new StringBuilder())
                    .toString();
            logger.debug(string);

            byte[] bytes = string.getBytes("UTF-8");
            MetricsData metricsData = JacksonUtil.getObjectMapper().readValue(bytes, MetricsData.class);
            assertTrue(metricsData.getGauges().size() == 1);
            assertTrue(metricsData.getGauge("testGauge").getValue() == 1);
            assertTrue(metricsData.getMeters().size() == 1);
            assertTrue(metricsData.getMeter("testMeter").getCount() == 1);
        } catch (Exception e) {
            logger.error("" + e, e);
            assertFalse(true);
        }
    }
}
