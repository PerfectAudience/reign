package io.reign.metrics;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;

/**
 * 
 * @author ypai
 * 
 */
public interface MetricRegistryManager {
    public MetricRegistry get();

    public void rotateAsNecessary();

    public int getRotationInterval();

    public TimeUnit getRotationTimeUnit();

    public long getLastRotatedTimestamp();
}
