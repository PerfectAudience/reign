package io.reign.metrics;

import io.reign.ReignContext;
import io.reign.data.DataService;
import io.reign.data.MultiMapData;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

/**
 * A reporter which writes to ZooKeeper. Based off CsvReporter code from Codahale Metrics. Not extending
 * ScheduledReporter to control threads.
 * 
 * @author ypai
 */
public class ZkMetricsReporter {
    /**
     * Returns a new {@link Builder} for {@link ZkMetricsReporter}.
     * 
     * @param registry
     *            the registry to report
     * @return a {@link Builder} instance for a {@link ZkMetricsReporter}
     */
    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    /**
     * A builder for {@link ZkMetricsReporter} instances. Defaults to using the default locale, converting rates to
     * events/second, converting durations to milliseconds, and not filtering metrics.
     */
    public static class Builder {
        private final MetricRegistry registry;
        private Locale locale;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private Clock clock;
        private MetricFilter filter;
        private ReignContext context;
        private String clusterId;
        private String serviceId;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.locale = Locale.getDefault();
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.clock = Clock.defaultClock();
            this.filter = MetricFilter.ALL;
        }

        public Builder context(ReignContext context) {
            this.context = context;
            return this;
        }

        public Builder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public Builder serviceId(String serviceId) {
            this.serviceId = serviceId;
            return this;
        }

        /**
         * Format numbers for the given {@link Locale}.
         * 
         * @param locale
         *            a {@link Locale}
         * @return {@code this}
         */
        public Builder formatFor(Locale locale) {
            this.locale = locale;
            return this;
        }

        /**
         * Convert rates to the given time unit.
         * 
         * @param rateUnit
         *            a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         * 
         * @param durationUnit
         *            a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Use the given {@link Clock} instance for the time.
         * 
         * @param clock
         *            a {@link Clock} instance
         * @return {@code this}
         */
        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         * 
         * @param filter
         *            a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        public ZkMetricsReporter build() {
            return new ZkMetricsReporter(registry, rateUnit, durationUnit, clock, filter, context, clusterId, serviceId);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(ZkMetricsReporter.class);
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private final MetricRegistry registry;
    private final MetricFilter filter;
    private final double durationFactor;
    private final String durationUnit;
    private final double rateFactor;
    private final String rateUnit;

    private final Clock clock;
    private final ReignContext context;
    private final String clusterId;
    private final String serviceId;
    private final MultiMapData<String> multiMapData;

    private ZkMetricsReporter(MetricRegistry registry, TimeUnit rateUnit, TimeUnit durationUnit, Clock clock,
            MetricFilter filter, ReignContext context, String clusterId, String serviceId) {

        this.registry = registry;
        this.filter = filter;
        this.rateFactor = rateUnit.toSeconds(1);
        this.rateUnit = calculateRateUnit(rateUnit);
        this.durationFactor = 1.0 / durationUnit.toNanos(1);
        this.durationUnit = durationUnit.toString().toLowerCase(Locale.US);

        this.clock = clock;
        this.context = context;
        this.clusterId = clusterId;
        this.serviceId = serviceId;

        DataService dataService = context.getService("data");
        String dataPath = context.getPathScheme().joinTokens(serviceId);
        multiMapData = dataService.getMultiMap(clusterId, dataPath);
    }

    public void report() {
        report(registry.getGauges(filter), registry.getCounters(filter), registry.getHistograms(filter),
                registry.getMeters(filter), registry.getTimers(filter));
    }

    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
            SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
        final long timestamp = TimeUnit.MILLISECONDS.toSeconds(clock.getTime());

        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            reportGauge(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            reportCounter(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            reportHistogram(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            reportMeter(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            reportTimer(timestamp, entry.getKey(), entry.getValue());
        }
    }

    private void reportTimer(long timestamp, String name, Timer timer) {
        final Snapshot snapshot = timer.getSnapshot();

        report(timestamp, name, new String[] { "count", "max", "mean", "min", "stddev", "p50", "p75", "p95", "p98",
                "p99", "p999", "mean_rate", "m1_rate", "m5_rate", "m15_rate", "rate_unit", "duration_unit" },
                timer.getCount(), convertDuration(snapshot.getMax()), convertDuration(snapshot.getMean()),
                convertDuration(snapshot.getMin()), convertDuration(snapshot.getStdDev()),
                convertDuration(snapshot.getMedian()), convertDuration(snapshot.get75thPercentile()),
                convertDuration(snapshot.get95thPercentile()), convertDuration(snapshot.get98thPercentile()),
                convertDuration(snapshot.get99thPercentile()), convertDuration(snapshot.get999thPercentile()),
                convertRate(timer.getMeanRate()), convertRate(timer.getOneMinuteRate()),
                convertRate(timer.getFiveMinuteRate()), convertRate(timer.getFifteenMinuteRate()), getRateUnit(),
                getDurationUnit());
    }

    private void reportMeter(long timestamp, String name, Meter meter) {
        report(timestamp, name, new String[] { "count", "mean_rate", "m1_rate", "m5_rate", "m15_rate", "rate_unit" },
                meter.getCount(), convertRate(meter.getMeanRate()), convertRate(meter.getOneMinuteRate()),
                convertRate(meter.getFiveMinuteRate()), convertRate(meter.getFifteenMinuteRate()), getRateUnit());
    }

    private void reportHistogram(long timestamp, String name, Histogram histogram) {
        final Snapshot snapshot = histogram.getSnapshot();
        report(timestamp, name, new String[] { "count", "max", "mean", "min", "stddev", "p50", "p75", "p95", "p98",
                "p99", "p999" }, histogram.getCount(), snapshot.getMax(), snapshot.getMean(), snapshot.getMin(),
                snapshot.getStdDev(), snapshot.getMedian(), snapshot.get75thPercentile(), snapshot.get95thPercentile(),
                snapshot.get98thPercentile(), snapshot.get99thPercentile(), snapshot.get999thPercentile());
    }

    private void reportCounter(long timestamp, String name, Counter counter) {
        report(timestamp, name, new String[] { "count" }, counter.getCount());
    }

    private void reportGauge(long timestamp, String name, Gauge gauge) {
        report(timestamp, name, new String[] { "value" }, gauge.getValue());
    }

    private void report(long timestamp, String name, String[] keys, Object... values) {
        StringBuilder sb = new StringBuilder();

        // encode owner of data
        sb.append("[ {\"");
        sb.append(context.getCanonicalIdProvider().forZk().getPathToken());
        sb.append("\"},\n");

        // encode metrics data
        sb.append("{\n");
        for (int i = 0; i < keys.length; i++) {
            String key = keys[i];
            sb.append("\"").append(key).append("\"");
            sb.append(" : ");
            sb.append("\"").append(values[i]).append("\"");
            sb.append(",");
        }
        sb.append("\n} ]");

        try {
            byte[] bytes = sb.toString().getBytes("UTF-8");
            multiMapData.put(name, bytes);

        } catch (UnsupportedEncodingException e) {
            logger.warn("" + e, e);
        }
    }

    protected String sanitize(String name) {
        return name;
    }

    protected String getRateUnit() {
        return rateUnit;
    }

    protected String getDurationUnit() {
        return durationUnit;
    }

    protected double convertDuration(double duration) {
        return duration * durationFactor;
    }

    protected double convertRate(double rate) {
        return rate * rateFactor;
    }

    private String calculateRateUnit(TimeUnit unit) {
        final String s = unit.toString().toLowerCase(Locale.US);
        return s.substring(0, s.length() - 1);
    }
}