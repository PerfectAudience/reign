package io.reign.metrics;

import com.codahale.metrics.Metric;

/**
 * A Metric that can be rotated or otherwise reset.
 * 
 * @author ypai
 * 
 */
public interface RotatingMetric {

    /**
     * 
     * @return a copy of the Metric representing the last value before rotating.
     */
    public Metric rotate();
}
