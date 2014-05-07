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

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;

/**
 * 
 * @author ypai
 * 
 */
public class StaticMetricRegistryManager implements MetricRegistryManager {

    private static final Logger logger = LoggerFactory.getLogger(StaticMetricRegistryManager.class);

    private final MetricRegistry metricRegistry;
    private final long lastRotatedTimestamp;

    public StaticMetricRegistryManager() {
        this.metricRegistry = new MetricRegistry();
        lastRotatedTimestamp = System.currentTimeMillis();
    }

    @Override
    public MetricRegistry get() {
        return this.metricRegistry;
    }

    @Override
    public int getRotationInterval() {
        return Integer.MAX_VALUE;
    }

    @Override
    public TimeUnit getRotationTimeUnit() {
        return TimeUnit.DAYS;
    }

    @Override
    public long getLastRotatedTimestamp() {
        return this.lastRotatedTimestamp;
    }

    @Override
    public MetricRegistry rotateAsNecessary() {
        return this.metricRegistry;
    }

}
