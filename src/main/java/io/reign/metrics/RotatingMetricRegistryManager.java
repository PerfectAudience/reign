/*
 Copyright 2013 Yen Pai ypai@reign.io

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package io.reign.metrics;

import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

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
public class RotatingMetricRegistryManager implements MetricRegistryManager {

    private static final Logger logger = LoggerFactory.getLogger(RotatingMetricRegistryManager.class);

    private volatile MetricRegistry metricRegistry;
    private volatile long lastRotatedTimestamp = 0L;

    private final int rotationInterval;
    private final TimeUnit rotationTimeUnit;
    private final long rotationIntervalMillis;

    private MetricRegistryManagerCallback callback = NullMetricRegistryManagerCallback.NULL_CALLBACK;

    public RotatingMetricRegistryManager(int rotationInterval, TimeUnit rotationTimeUnit) {
        this.rotationInterval = rotationInterval;
        this.rotationTimeUnit = rotationTimeUnit;
        rotationIntervalMillis = rotationTimeUnit.toMillis(rotationInterval);
        rotateAsNecessary();
    }

    public RotatingMetricRegistryManager(int rotationInterval, TimeUnit rotationTimeUnit,
            MetricRegistryManagerCallback callback) {
        this(rotationInterval, rotationTimeUnit);
        this.callback = callback;
    }

    @Override
    public Counter counter(String name) {
        return this.metricRegistry.counter(name);
    }

    @Override
    public Meter meter(String name) {
        return this.metricRegistry.meter(name);
    }

    @Override
    public Timer timer(String name) {
        return this.metricRegistry.timer(name);
    }

    @Override
    public Histogram histogram(String name) {
        return this.metricRegistry.histogram(name);
    }

    @Override
    public Gauge gauge(String name, Gauge gauge) {
        this.metricRegistry.register(name, gauge);
        return gauge;
    }

    @Override
    public MetricRegistry get() {
        return this.metricRegistry;
    }

    @Override
    public int getRotationInterval() {
        return this.rotationInterval;
    }

    @Override
    public TimeUnit getRotationTimeUnit() {
        return this.rotationTimeUnit;
    }

    @Override
    public long getLastRotatedTimestamp() {
        return this.lastRotatedTimestamp;
    }

    @Override
    public synchronized MetricRegistry rotateAsNecessary() {
        if (System.currentTimeMillis() - lastRotatedTimestamp > rotationIntervalMillis) {
            MetricRegistry oldMetricRegistry = this.metricRegistry;
            this.metricRegistry = new MetricRegistry();
            this.lastRotatedTimestamp = getNormalizedTimestamp(rotationIntervalMillis);

            if (oldMetricRegistry != null) {
                callback.rotated(metricRegistry, oldMetricRegistry);
            }

            logger.debug(
                    "Rotating MetricRegistry:  System.currentTimeMillis()={}; lastRotatedTimestamp={}; rotationIntervalMillis={}; lastRotatedTimestamp={}",
                    System.currentTimeMillis(), lastRotatedTimestamp, rotationIntervalMillis, lastRotatedTimestamp);
        }
        return this.metricRegistry;
    }

    long getNormalizedTimestamp(long intervalLength) {
        long currentTimestamp = System.currentTimeMillis();
        long intervalStartTimestamp;

        /***** check interval lengths and create a normalized starting point, so all nodes are on the same interval clock *****/
        if (intervalLength >= 3600000) {
            // interval >= hour, set to previous hour start point
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            cal.setTimeInMillis(currentTimestamp);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            intervalStartTimestamp = cal.getTimeInMillis();
        } else if (intervalLength >= 1800000) {
            // interval >= 30 minutes, set to previous half-hour start point
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            cal.setTimeInMillis(currentTimestamp);

            if (cal.get(Calendar.MINUTE) >= 30) {
                cal.set(Calendar.MINUTE, 30);
            } else {
                cal.set(Calendar.MINUTE, 0);
            }

            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            intervalStartTimestamp = cal.getTimeInMillis();
        } else if (intervalLength >= 900000) {
            // interval >= 15 minutes, set to nearest previous quarter hour
            // start point
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            cal.setTimeInMillis(currentTimestamp);

            int minutes = cal.get(Calendar.MINUTE);
            int diff = minutes % 15;
            cal.set(Calendar.MINUTE, minutes - diff);

            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            intervalStartTimestamp = cal.getTimeInMillis();

        } else if (intervalLength >= 600000) {
            // interval >= 10 minutes, set to nearest previous 10 minute
            // start point
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            cal.setTimeInMillis(currentTimestamp);

            int minutes = cal.get(Calendar.MINUTE);
            int diff = minutes % 10;
            cal.set(Calendar.MINUTE, minutes - diff);

            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            intervalStartTimestamp = cal.getTimeInMillis();

        } else if (intervalLength >= 300000) {
            // interval >= 5 minutes, set to nearest previous 5 minute start
            // point
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            cal.setTimeInMillis(currentTimestamp);

            int minutes = cal.get(Calendar.MINUTE);
            int diff = minutes % 5;
            cal.set(Calendar.MINUTE, minutes - diff);

            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            intervalStartTimestamp = cal.getTimeInMillis();

        } else if (intervalLength >= 60000) {
            // interval >= 1 minute, set to nearest previous minute start point
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            cal.setTimeInMillis(currentTimestamp);
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            intervalStartTimestamp = cal.getTimeInMillis();
        } else {
            // smaller resolutions we just start whenever
            intervalStartTimestamp = currentTimestamp;
        }

        return intervalStartTimestamp;
    }
}
