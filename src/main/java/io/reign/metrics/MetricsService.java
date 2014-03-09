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
import io.reign.PathScheme;
import io.reign.PathType;
import io.reign.data.DataService;
import io.reign.data.MultiMapData;
import io.reign.util.ZkClientUtil;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
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

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    public static final int DEFAULT_UPDATE_INTERVAL_MILLIS = 15000;

    private int updateIntervalMillis = DEFAULT_UPDATE_INTERVAL_MILLIS;

    private final Map<String, String> exportPathMap = new ConcurrentHashMap<String, String>(16, 0.9f, 1);

    private final ZkClientUtil zkClientUtil = new ZkClientUtil();

    private ScheduledExecutorService executorService;

    public void exportMetrics(final String clusterId, final String serviceId, MetricRegistry registry,
            long updateInterval, TimeUnit updateIntervalTimeUnit) {

        final String key = clusterId + "/" + serviceId;
        synchronized (this) {
            if (!exportPathMap.containsKey(key)) {
                exportPathMap.put(key, "");
            } else {
                logger.info("Already exported metrics:  {}", key);
                return;
            }
        }

        final ZkMetricsReporter reporter = ZkMetricsReporter.forRegistry(registry).convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS).build();

        executorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                logger.trace("EXPORTING METRICS...");
                StringBuilder sb = new StringBuilder();
                sb = reporter.report(sb);
                logger.trace("EXPORTING METRICS:  {}", sb);

                PathScheme pathScheme = getContext().getPathScheme();
                String dataPathPrefix = pathScheme.getAbsolutePath(
                        PathType.DATA,
                        pathScheme.joinTokens(clusterId, serviceId, getContext().getCanonicalIdProvider().forZk()
                                .getPathToken()));

                String dataPath = exportPathMap.get(key);
                try {
                    if (dataPath == null || "".equals(dataPath)) {
                        dataPath = zkClientUtil.updatePath(getContext().getZkClient(), getContext().getPathScheme(),
                                dataPathPrefix + "-", sb.toString().getBytes(UTF_8),
                                getContext().getDefaultZkAclList(), CreateMode.PERSISTENT_SEQUENTIAL, -1);
                        exportPathMap.put(key, dataPath);
                    } else {
                        dataPath = zkClientUtil.updatePath(getContext().getZkClient(), getContext().getPathScheme(),
                                dataPath, sb.toString().getBytes(UTF_8), getContext().getDefaultZkAclList(),
                                CreateMode.PERSISTENT, -1);
                    }
                    logger.debug("Updated metrics data:  dataPath={}", dataPath);
                } catch (Exception e) {
                    logger.error("Could not export metrics data:  pathPrefix=" + dataPath, e);
                }

                // DataService dataService = getContext().getService("data");
                // logger.trace("Got data service");
                // MultiMapData<String> multiMapData = dataService.getMultiMap(clusterId, serviceId);
                // logger.trace("Got multimap");
                // try {
                // multiMapData.put(getContext().getCanonicalIdProvider().forZk().getPathToken(), "metrics", sb
                // .toString().getBytes("UTF-8"));
                // } catch (UnsupportedEncodingException e) {
                // logger.error("" + e, e);
                // }
            }
        }, 0, updateInterval, updateIntervalTimeUnit);
    }

    public MetricsData getMetrics(String clusterId, String serviceId) {
        return getMetrics(clusterId, serviceId, getContext().getCanonicalIdProvider().forZk().getPathToken());
    }

    public MetricsData getMetrics(String clusterId, String serviceId, String nodeId) {
        DataService dataService = getContext().getService("data");
        MultiMapData<String> multiMapData = dataService.getMultiMap(clusterId, serviceId);
        MetricsData metricsData = multiMapData.get(nodeId, MetricsData.class);
        return metricsData;
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
            // list all services in cluster

            // get lock for a service

            // get all data nodes for a service

            // aggregate service stats

            // gauges: weighted avg

            // counters: sum

            // histogram: sum count, take max, take min, weighted avg of averages, sqrt of sum of variances (sum of
            // each stddev squared), weighted avg. of percentiles

            // timers: treat like histograms

            // meters: sum count, weighted avg. of results

            // store aggregated results in ZK at service level

        }
    }

}
