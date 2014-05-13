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

import io.reign.util.TimeUnitUtil;

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
        long currentTimestamp = System.currentTimeMillis();
        if (currentTimestamp - lastRotatedTimestamp > rotationIntervalMillis) {
            MetricRegistry oldMetricRegistry = this.metricRegistry;
            this.metricRegistry = new MetricRegistry();
            this.lastRotatedTimestamp = TimeUnitUtil.getNormalizedIntervalStartTimestamp(rotationIntervalMillis,
                    System.currentTimeMillis());

            if (oldMetricRegistry != null) {
                callback.rotated(metricRegistry, oldMetricRegistry);
            }

            logger.debug(
                    "Rotating MetricRegistry:  System.currentTimeMillis()={}; lastRotatedTimestamp={}; rotationIntervalMillis={}; lastRotatedTimestamp={}",
                    System.currentTimeMillis(), lastRotatedTimestamp, rotationIntervalMillis, lastRotatedTimestamp);
        }
        return this.metricRegistry;
    }

}
