package io.reign.metrics;

import com.codahale.metrics.MetricRegistry;

/**
 * 
 * @author ypai
 * 
 */
public class NullMetricRegistryManagerCallback implements MetricRegistryManagerCallback {

    public static final NullMetricRegistryManagerCallback NULL_CALLBACK = new NullMetricRegistryManagerCallback();

    @Override
    public void rotated(MetricRegistry current, MetricRegistry previous) {

    }

}
