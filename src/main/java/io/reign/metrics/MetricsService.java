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

import io.reign.AbstractService;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;

/**
 * 
 * @author ypai
 * 
 */
public class MetricsService extends AbstractService {

    private static final Logger logger = LoggerFactory.getLogger(MetricsService.class);

    public static final int DEFAULT_UPDATE_INTERVAL_MILLIS = 15000;

    private int updateIntervalMillis = DEFAULT_UPDATE_INTERVAL_MILLIS;

    private ScheduledExecutorService executorService;

    public void export(String clusterId, String serviceId, MetricRegistry registry, long interval,
            TimeUnit intervalTimeUnit) {
        final ZkMetricsReporter reporter = ZkMetricsReporter.forRegistry(registry).convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS).build();
        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                reporter.report();
            }
        }, 0, interval, intervalTimeUnit);
    }

    public Map<String, Object> get(String clusterId, String serviceId) {
        return null;
    }

    public void setUpdateIntervalMillis(int updateIntervalMillis) {
        this.updateIntervalMillis = updateIntervalMillis;
    }

    public int getUpdateIntervalMillis() {
        return updateIntervalMillis;
    }

    @Override
    public synchronized void init() {
        if (executorService != null) {
            return;
        }

        logger.info("init() called");

        executorService = new ScheduledThreadPoolExecutor(2);

        // schedule admin activity
        Runnable adminRunnable = new AdminRunnable();
        executorService.scheduleAtFixedRate(adminRunnable, this.updateIntervalMillis / 2, this.updateIntervalMillis,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void destroy() {
        executorService.shutdown();
    }

    public class AdminRunnable implements Runnable {
        @Override
        public void run() {
            // get lock

            // get all nodes

            // aggregate service stats and store in cache

        }
    }

}
