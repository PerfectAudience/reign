package io.reign.metrics;

import static org.junit.Assert.assertTrue;
import io.reign.MasterTestSuite;
import io.reign.presence.PresenceService;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

public class MetricsServiceTest {
    private static final Logger logger = LoggerFactory.getLogger(MetricsServiceTest.class);

    private MetricsService metricsService;
    private PresenceService presenceService;

    @Before
    public void setUp() throws Exception {
        metricsService = MasterTestSuite.getReign().getService("metrics");
        metricsService.setUpdateIntervalMillis(3000);

        presenceService = MasterTestSuite.getReign().getService("presence");
        presenceService.announce("clusterA", "serviceA", true);
        presenceService.announce("clusterA", "serviceB", true);
        presenceService.announce("clusterA", "serviceC", true);
    }

    @Test
    public void testObserver() throws Exception {
        MetricRegistryManager registryManager = getMetricRegistryManager();
        metricsService.scheduleExport("clusterA", "serviceC", registryManager, 1, TimeUnit.SECONDS);

        final AtomicInteger calledCount = new AtomicInteger(0);
        metricsService.observe("clusterA", "serviceC", new MetricsObserver() {

            @Override
            public void updated(MetricsData updated, MetricsData previous) {
                logger.debug("*** OBSERVER:  updated={}; previous={}", updated, previous);
                calledCount.incrementAndGet();

            }

        });

        // has not been updated yet
        assertTrue("calledCount should be 0, but is " + calledCount.get(), calledCount.get() == 0);

        // will have been updated
        Thread.sleep((metricsService.getUpdateIntervalMillis() / 2) + 1000);
        assertTrue(calledCount.get() == 1);

        // force a change so observer will be called again
        Counter counter1 = registryManager.get().counter("counter1");
        counter1.inc();
        Thread.sleep(metricsService.getUpdateIntervalMillis());
        assertTrue(calledCount.get() == 2);

    }

    @Test
    public void testExportSelfMetrics() throws Exception {
        MetricRegistryManager registryManager = getMetricRegistryManager();
        metricsService.scheduleExport("clusterA", "serviceA", registryManager, 5, TimeUnit.SECONDS);

        MetricsData metricsData = metricsService.getMetrics("clusterA", "serviceA");
        while (metricsData == null) {
            metricsData = metricsService.getMetrics("clusterA", "serviceA");
        }

        // counters
        CounterData counter1Data = metricsData.getCounter("counter1");
        CounterData counter2Data = metricsData.getCounter("counter2");
        assertTrue(counter1Data.getCount() == 1L);
        assertTrue(counter2Data.getCount() == 2L);

        // gauges
        GaugeData gauge1 = metricsData.getGauge("gauge1");
        GaugeData gauge2 = metricsData.getGauge("gauge2");
        assertTrue(gauge1.getValue() == 1.0);
        assertTrue(gauge2.getValue() == 2.0);

        // meters
        MeterData meter1 = metricsData.getMeter("meter1");
        MeterData meter2 = metricsData.getMeter("meter2");
        assertTrue(meter1.getCount() == 1000);
        assertTrue(meter2.getCount() == 4000);

        // timers
        TimerData timer1 = metricsData.getTimer("timer1");
        TimerData timer2 = metricsData.getTimer("timer2");
        assertTrue(timer1.getCount() == 1);
        assertTrue(timer2.getCount() == 2);
        assertTrue(Math.round(timer1.getMax()) == 100);
        assertTrue(Math.round(timer2.getMax()) == 200);

        // histograms
        HistogramData histo1 = metricsData.getHistogram("histo1");
        HistogramData histo2 = metricsData.getHistogram("histo2");
        assertTrue(histo1.getCount() == 3);
        assertTrue(histo1.getMin() == 10);
        assertTrue(histo1.getMean() == 20);
        assertTrue(histo1.getMax() == 30);
        assertTrue(histo1.getP999() == 30);
        assertTrue(histo2.getCount() == 6);
        assertTrue(histo2.getMin() == 100);
        assertTrue(histo2.getMean() == 350);
        assertTrue(histo2.getMax() == 600);
        assertTrue(histo2.getP999() == 600);
    }

    @Test
    public void testExportServiceMetrics() throws Exception {
        MetricRegistryManager registryManager1 = getMetricRegistryManager();
        metricsService.scheduleExport("clusterA", "serviceB", "node1", registryManager1, 1, TimeUnit.SECONDS);

        MetricRegistryManager registryManager2 = getMetricRegistryManager();
        metricsService.scheduleExport("clusterA", "serviceB", "node2", registryManager2, 1, TimeUnit.SECONDS);

        // get service metrics, but wait for both service nodes to be there before checking values
        MetricsData metricsData = null;
        while ((metricsData = metricsService.getServiceMetrics("clusterA", "serviceB")) == null
                || metricsData.getNodeCount() < 2) {
            // wait for aggregation to happen
            Thread.sleep(metricsService.getUpdateIntervalMillis() / 2);
        }

        // counters
        CounterData counter1Data = metricsData.getCounter("counter1");
        CounterData counter2Data = metricsData.getCounter("counter2");
        assertTrue("Unexpected value:  " + counter1Data.getCount(), counter1Data.getCount() == 2L);
        assertTrue(counter2Data.getCount() == 4L);

        // gauges
        GaugeData gauge1 = metricsData.getGauge("gauge1");
        GaugeData gauge2 = metricsData.getGauge("gauge2");
        assertTrue(gauge1.getValue() == 1.0);
        assertTrue(gauge2.getValue() == 2.0);

        // meters
        MeterData meter1 = metricsData.getMeter("meter1");
        MeterData meter2 = metricsData.getMeter("meter2");
        assertTrue(meter1.getCount() == 2000);
        assertTrue(meter2.getCount() == 8000);

        // timers
        TimerData timer1 = metricsData.getTimer("timer1");
        TimerData timer2 = metricsData.getTimer("timer2");
        assertTrue(timer1.getCount() == 2);
        assertTrue(timer2.getCount() == 4);
        assertTrue(Math.round(timer1.getMax()) == 100);
        assertTrue(Math.round(timer2.getMax()) == 200);

        // histograms
        HistogramData histo1 = metricsData.getHistogram("histo1");
        HistogramData histo2 = metricsData.getHistogram("histo2");
        assertTrue(histo1.getCount() == 6);
        assertTrue(histo1.getMin() == 10);
        assertTrue(histo1.getMean() == 20);
        assertTrue(histo1.getMax() == 30);
        assertTrue(histo1.getP999() == 30);
        assertTrue(histo2.getCount() == 12);
        assertTrue(histo2.getMin() == 100);
        assertTrue(histo2.getMean() == 350);
        assertTrue(histo2.getMax() == 600);
        assertTrue(histo2.getP999() == 600);

    }

    MetricRegistryManager getMetricRegistryManager() throws Exception {
        RotatingMetricRegistryManager registryManager = new RotatingMetricRegistryManager(300, TimeUnit.SECONDS);

        // counters
        Counter counter1 = registryManager.get().counter(MetricRegistry.name("counter1"));
        Counter counter2 = registryManager.get().counter(MetricRegistry.name("counter2"));
        counter1.inc();
        counter2.inc(2);

        // gauges
        Gauge<Integer> gauge1 = registryManager.get().register(MetricRegistry.name("gauge1"), new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return 1;
            }
        });
        Gauge<Integer> gauge2 = registryManager.get().register(MetricRegistry.name("gauge2"), new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return 2;
            }
        });

        // meters
        Meter meter1 = registryManager.get().meter(MetricRegistry.name("meter1"));
        Meter meter2 = registryManager.get().meter(MetricRegistry.name("meter2"));

        meter1.mark(1000);
        meter2.mark(2000);
        meter2.mark(2000);

        // wait 5 secs after initial mark so meters can fill out some rates
        Thread.sleep(5000);

        // timers
        Timer timer1 = registryManager.get().timer("timer1");
        Timer timer2 = registryManager.get().timer("timer2");
        final Timer.Context context1 = timer1.time();
        try {
            Thread.sleep(100);
        } finally {
            context1.stop();
        }
        final Timer.Context context2 = timer2.time();
        try {
            Thread.sleep(200);
        } finally {
            context2.stop();
        }
        final Timer.Context context3 = timer2.time();
        try {
            Thread.sleep(200);
        } finally {
            context3.stop();
        }

        // histograms
        Histogram histo1 = registryManager.get().histogram("histo1");
        Histogram histo2 = registryManager.get().histogram("histo2");
        histo1.update(10);
        histo1.update(20);
        histo1.update(30);
        histo2.update(100);
        histo2.update(200);
        histo2.update(300);
        histo2.update(400);
        histo2.update(500);
        histo2.update(600);

        return registryManager;
    }
}
