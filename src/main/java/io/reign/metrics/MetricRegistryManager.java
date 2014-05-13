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
public interface MetricRegistryManager {

    public Counter counter(String name);

    public Meter meter(String name);

    public Timer timer(String name);

    public Histogram histogram(String name);

    public Gauge gauge(String name, Gauge gauge);

    public MetricRegistry get();

    /**
     * 
     * @return old MetricRegistry if rotated; current MetricRegistry otherwise
     */
    public MetricRegistry rotateAsNecessary();

    public int getRotationInterval();

    public TimeUnit getRotationTimeUnit();

    public long getLastRotatedTimestamp();
}
