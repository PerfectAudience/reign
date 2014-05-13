package io.reign.metrics;

import static org.junit.Assert.assertTrue;
import io.reign.MasterTestSuite;
import io.reign.presence.PresenceService;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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

/**
 * 
 * @author ypai
 * 
 */
public class RotatingMetricRegistryManagerTest {
    private static final Logger logger = LoggerFactory.getLogger(RotatingMetricRegistryManagerTest.class);

    private MetricsService metricsService;
    private PresenceService presenceService;

    @Before
    public void setUp() throws Exception {

        metricsService = MasterTestSuite.getReign().getService("metrics");
        metricsService.setUpdateIntervalMillis(3000);

        presenceService = MasterTestSuite.getReign().getService("presence");
        presenceService.announce("clusterZ", "serviceZ", true);
    }

    @Test
    public void testCallback() throws Exception {
        final AtomicBoolean testPassed = new AtomicBoolean(false);
        final AtomicReference<MetricRegistry> previousRef = new AtomicReference<MetricRegistry>();
        final AtomicReference<MetricRegistry> currentRef = new AtomicReference<MetricRegistry>();

        MetricRegistryManager registryManager = getMetricRegistryManager(new RotatingMetricRegistryManager(1,
                TimeUnit.SECONDS, new MetricRegistryManagerCallback() {
                    @Override
                    public void rotated(MetricRegistry current, MetricRegistry previous) {
                        logger.debug("previous.counter1={}; current.counter1={}", previous.counter("counter1")
                                .getCount(), current.counter("counter1").getCount());
                        if (!testPassed.get()) {
                            previousRef.set(previous);
                            currentRef.set(current);
                            testPassed.set(previous.counter("counter1").getCount() == 1
                                    && current.counter("counter1").getCount() == 0);
                            synchronized (testPassed) {
                                testPassed.notifyAll();
                            }
                        }
                    }
                }));
        metricsService.scheduleExport("clusterZ", "serviceZ", registryManager, 1, TimeUnit.SECONDS);

        synchronized (testPassed) {
            testPassed.wait(10000);
        }

        assertTrue("previous=" + previousRef.get() + "; current=" + currentRef.get(), previousRef.get() != null
                && currentRef.get() != null);
        assertTrue("previous.counter1=" + previousRef.get().counter("counter1") + "; current.counter1="
                + currentRef.get().counter("counter1").getCount(), testPassed.get());
    }

    MetricRegistryManager getMetricRegistryManager(MetricRegistryManager registryManager) throws Exception {
        // RotatingMetricRegistryManager registryManager = new RotatingMetricRegistryManager(300, TimeUnit.SECONDS);
        // MetricRegistryManager registryManager = new StaticMetricRegistryManager();

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
