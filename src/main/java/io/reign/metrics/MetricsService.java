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
import io.reign.JsonDataSerializer;
import io.reign.PathScheme;
import io.reign.PathType;
import io.reign.ReignException;
import io.reign.util.JacksonUtil;
import io.reign.util.ZkClientUtil;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class MetricsService extends AbstractService {

    private static final Logger logger = LoggerFactory.getLogger(MetricsService.class);

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    public static final int DEFAULT_UPDATE_INTERVAL_MILLIS = 15000;

    private static final JsonDataSerializer<MetricsData> jsonSerializer = new JsonDataSerializer<MetricsData>();

    /** interval btw. aggregations at service level */
    private int updateIntervalMillis = DEFAULT_UPDATE_INTERVAL_MILLIS;

    private final Map<String, ExportMeta> exportPathMap = new ConcurrentHashMap<String, ExportMeta>(16, 0.9f, 1);

    private static class ExportMeta {
        public String dataPath;
        public RotatingMetricRegistryRef registryRef;

        public ExportMeta(String dataPath, RotatingMetricRegistryRef registryRef) {
            this.dataPath = dataPath;
            this.registryRef = registryRef;
        }
    }

    private final ZkClientUtil zkClientUtil = new ZkClientUtil();

    private ScheduledExecutorService executorService;

    public void exportMetrics(final String clusterId, final String serviceId,
            final RotatingMetricRegistryRef registryRef, long updateInterval, TimeUnit updateIntervalTimeUnit) {

        final String key = clusterId + "/" + serviceId;
        synchronized (this) {
            if (!exportPathMap.containsKey(key)) {
                exportPathMap.put(key, new ExportMeta(null, registryRef));
            } else {
                logger.info("Already exported metrics:  {}", key);
                return;
            }
        }

        final ZkMetricsReporter reporter = ZkMetricsReporter.forRegistry(registryRef).convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS).build();

        executorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                // logger.trace("EXPORTING METRICS...");
                StringBuilder sb = new StringBuilder();
                sb = reporter.report(sb);
                logger.trace("EXPORTING METRICS:  {}", sb);

                // export to zk
                ExportMeta exportMeta = exportPathMap.get(key);
                try {
                    if (exportMeta == null || exportMeta.dataPath == null) {
                        PathScheme pathScheme = getContext().getPathScheme();
                        String dataPathPrefix = pathScheme.getAbsolutePath(
                                PathType.DATA,
                                pathScheme.joinTokens(clusterId, serviceId, getContext().getCanonicalIdProvider()
                                        .forZk().getPathToken()));
                        exportMeta.dataPath = zkClientUtil.updatePath(getContext().getZkClient(), getContext()
                                .getPathScheme(), dataPathPrefix + "-", sb.toString().getBytes(UTF_8), getContext()
                                .getDefaultZkAclList(), CreateMode.PERSISTENT_SEQUENTIAL, -1);
                        exportPathMap.put(key, exportMeta);
                    } else {
                        zkClientUtil.updatePath(getContext().getZkClient(), getContext().getPathScheme(),
                                exportMeta.dataPath, sb.toString().getBytes(UTF_8), getContext().getDefaultZkAclList(),
                                CreateMode.PERSISTENT, -1);
                    }

                    logger.debug("Updated metrics data:  dataPath={}", exportMeta.dataPath);
                } catch (Exception e) {
                    logger.error("Could not export metrics data:  pathPrefix=" + exportMeta.dataPath, e);
                }

                // rotate as necessary
                registryRef.rotateAsNecessary();

            }
        }, 0, updateInterval, updateIntervalTimeUnit);
    }

    public MetricsData getServiceMetrics(String clusterId, String serviceId) {
        try {
            PathScheme pathScheme = getContext().getPathScheme();
            String dataPath = pathScheme.getAbsolutePath(PathType.DATA, pathScheme.joinTokens(clusterId, serviceId));
            byte[] bytes = getContext().getZkClient().getData(dataPath, false, new Stat());
            // String metricsDataJson = new String(bytes, UTF_8);
            // metricsDataJson.replaceAll("\n", "");
            MetricsData metricsData = JacksonUtil.getObjectMapper().readValue(bytes, MetricsData.class);
            return metricsData;
        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NODEEXISTS) {
                return null;
            }
            throw new ReignException(e);
        } catch (Exception e) {
            throw new ReignException(e);
        }
    }

    public MetricsData getMetrics(String clusterId, String serviceId) {
        String key = clusterId + "/" + serviceId;
        ExportMeta exportMeta = exportPathMap.get(key);
        if (exportMeta == null || exportMeta.dataPath == null) {
            logger.trace("MetricsData not found:  clusterId={}; serviceId={}", clusterId, serviceId);
            return null;
        }

        try {
            logger.debug("Retrieving metrics:  path={}", exportMeta.dataPath);
            byte[] bytes = getContext().getZkClient().getData(exportMeta.dataPath, true, new Stat());
            MetricsData metricsData = JacksonUtil.getObjectMapper().readValue(bytes, MetricsData.class);
            return metricsData;
        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NODEEXISTS) {
                return null;
            }
            throw new ReignException(e);
        } catch (Exception e) {
            throw new ReignException(e);
        }

    }

    public MetricsData getMetrics(String clusterId, String serviceId, String nodeId) {
        try {
            PathScheme pathScheme = getContext().getPathScheme();
            String dataPath = pathScheme.getAbsolutePath(
                    PathType.DATA,
                    pathScheme.joinTokens(clusterId, serviceId, getContext().getCanonicalIdProvider().forZk()
                            .getPathToken()));
            byte[] bytes = getContext().getZkClient().getData(dataPath, false, new Stat());
            MetricsData metricsData = JacksonUtil.getObjectMapper().readValue(bytes, MetricsData.class);
            return metricsData;
        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NODEEXISTS) {
                return null;
            }
            throw new ReignException(e);
        } catch (Exception e) {
            throw new ReignException(e);
        }
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
        Runnable adminRunnable = new AggregationRunnable();
        executorService.scheduleAtFixedRate(adminRunnable, this.updateIntervalMillis / 2, this.updateIntervalMillis,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void destroy() {
        executorService.shutdown();
    }

    public class AggregationRunnable implements Runnable {
        @Override
        public void run() {
            // list all services in cluster

            // get lock for a service -- should only perform if in cluster

            // get all data nodes for a service

            /** aggregate service stats for data nodes that within current rotation interval **/

            // gauges: weighted avg

            // counters: sum

            // histogram: sum count, take max, take min, weighted avg of averages, sqrt of sum of variances (sum of
            // each stddev squared), weighted avg. of percentiles

            // timers: treat like histograms

            // meters: sum count, weighted avg. of results

            // store aggregated results in ZK at service level

        }
    }

    public class CleanerRunnable implements Runnable {
        @Override
        public void run() {

            // list all services in cluster

            // get lock for a service -- should only perform if in cluster

            // get all data nodes for a service

            /** remove all nodes that are older than rotation interval **/
        }
    }

}
