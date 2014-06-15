/*
 * Copyright 2013 Yen Pai ypai@reign.io
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.reign.metrics;

import io.reign.util.TimeUnitUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

/**
 * 
 * @author ypai
 * 
 */
public class RotatingMetricRegistryManager implements MetricRegistryManager {

    private static final Logger logger = LoggerFactory.getLogger(RotatingMetricRegistryManager.class);

    private final MetricRegistry metricRegistry;
    private volatile long lastRotatedTimestamp = 0L;

    private final int rotationInterval;
    private final TimeUnit rotationTimeUnit;
    private final long rotationIntervalMillis;
    private List<MetricRegistryManagerCallback> callbackList = Collections.EMPTY_LIST;

    public RotatingMetricRegistryManager(int rotationInterval, TimeUnit rotationTimeUnit) {
        this.rotationInterval = rotationInterval;
        this.rotationTimeUnit = rotationTimeUnit;
        rotationIntervalMillis = rotationTimeUnit.toMillis(rotationInterval);

        metricRegistry = new MetricRegistry();
        lastRotatedTimestamp = TimeUnitUtil.getNormalizedIntervalStartTimestamp(rotationIntervalMillis,
                System.currentTimeMillis());

    }

    public RotatingMetricRegistryManager(int rotationInterval, TimeUnit rotationTimeUnit,
            MetricRegistryManagerCallback callback) {
        this(rotationInterval, rotationTimeUnit);
        registerCallback(callback);
    }

    public RotatingMetricRegistryManager(int rotationInterval, TimeUnit rotationTimeUnit,
            List<MetricRegistryManagerCallback> callbackList) {
        this(rotationInterval, rotationTimeUnit);
        this.callbackList = callbackList;
    }

    @Override
    public void registerCallback(MetricRegistryManagerCallback callback) {
        synchronized (callbackList) {
            if (callbackList == Collections.EMPTY_LIST) {
                callbackList = new ArrayList<MetricRegistryManagerCallback>();
            }
            if (callbackList.contains(callback)) {
                return;
            }
            callbackList.add(callback);
        }
    }

    @Override
    public void removeCallback(MetricRegistryManagerCallback callback) {
        synchronized (callbackList) {
            if (callbackList == Collections.EMPTY_LIST) {
                return;
            }
            callbackList.remove(callback);
        }
    }

    @Override
    public void removeAllCallbacks() {
        synchronized (callbackList) {
            if (callbackList == Collections.EMPTY_LIST) {
                return;
            }
            callbackList.clear();
        }
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
        MetricRegistry outputMetricRegistry = this.metricRegistry;

        long currentTimestamp = System.currentTimeMillis();
        if (currentTimestamp - lastRotatedTimestamp > rotationIntervalMillis) {

            logger.debug(
                    "Rotating MetricRegistry:  currentTimestamp={}; lastRotatedTimestamp={}; rotationIntervalMillis={}; lastRotatedTimestamp={}",
                    currentTimestamp, lastRotatedTimestamp, rotationIntervalMillis, lastRotatedTimestamp);

            // migrate metrics to outputMetricRegistry to ensure caller is able to flush persist current state
            outputMetricRegistry = new MetricRegistry();

            // reset metrics
            Map<String, Metric> metricMap = this.metricRegistry.getMetrics();
            Set<String> keys = metricMap.keySet();
            for (String key : keys) {
                String metricName = key;
                Metric metric = metricMap.get(metricName);

                if (metric instanceof RotatingMetric) {
                    Metric oldValue = ((RotatingMetric) metric).rotate();
                    outputMetricRegistry.register(metricName, oldValue);

                } else if (metric instanceof Counter) {
                    Counter counter = (Counter) metric;

                    // add counter with current value to outputMetricRegistry
                    long count = counter.getCount();
                    Counter counterCopy = outputMetricRegistry.counter(metricName);
                    counterCopy.inc(count);

                    // decrement "live" counter with count
                    counter.dec(count);
                } else if (metric instanceof Gauge) {
                    // save old one to copy that will be returned
                    outputMetricRegistry.register(metricName, metric);

                    // we don't rotate gauges unless they are marked RotatingMetric

                } else if (metric instanceof Histogram) {
                    // save old one to copy that will be returned
                    outputMetricRegistry.register(metricName, metric);

                    // rotate in new
                    if (!metricRegistry.remove(metricName)) {
                        logger.warn("Metric was not removed:  metricName={}", metricName);
                    }
                    metricRegistry.histogram(metricName);

                } else if (metric instanceof Meter) {
                    // save old one to copy that will be returned
                    outputMetricRegistry.register(metricName, metric);

                    // rotate in new
                    if (!metricRegistry.remove(metricName)) {
                        logger.warn("Metric was not removed:  metricName={}", metricName);
                    }
                    metricRegistry.meter(metricName);

                } else if (metric instanceof Timer) {
                    // save old one to copy that will be returned
                    outputMetricRegistry.register(metricName, metric);

                    // rotate in new
                    if (!metricRegistry.remove(metricName)) {
                        logger.warn("Metric was not removed:  metricName={}", metricName);
                    }
                    metricRegistry.timer(metricName);
                }
            }// for

            // set rotated timestamp
            this.lastRotatedTimestamp = TimeUnitUtil.getNormalizedIntervalStartTimestamp(rotationIntervalMillis,
                    currentTimestamp);

            synchronized (callbackList) {
                for (MetricRegistryManagerCallback callback : callbackList) {
                    callback.rotated(metricRegistry, outputMetricRegistry);
                }
            }

        }

        return outputMetricRegistry;
    }
}
