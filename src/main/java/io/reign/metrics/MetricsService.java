/*
 * Copyright 2014 Yen Pai ypai@reign.io
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

import io.reign.Service;

import java.util.concurrent.TimeUnit;

/**
 * 
 * @author ypai
 * 
 */
public interface MetricsService extends Service {

    public void observe(final String clusterId, final String serviceId, MetricsObserver observer);

    public MetricRegistryManager getRegistered(String clusterId, String serviceId);

    /**
     * Registers metrics for export to ZK.
     */
    public void scheduleExport(final String clusterId, final String serviceId,
            final MetricRegistryManager registryManager, long updateInterval, TimeUnit updateIntervalTimeUnit);

    /**
     * Get metrics data for given service.
     */
    public MetricsData getServiceMetrics(String clusterId, String serviceId);

    /**
     * Get metrics data for this service node (self) for current interval.
     */
    public MetricsData getMyMetrics(String clusterId, String serviceId);

    public void setAggregationIntervalMillis(int aggregationIntervalMillis);

    public int getAggregationIntervalMillis();

}
