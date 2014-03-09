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
import io.reign.ZkClient;
import io.reign.coord.CoordinationService;
import io.reign.coord.DistributedLock;
import io.reign.presence.PresenceService;
import io.reign.util.JacksonUtil;
import io.reign.util.ZkClientUtil;

import java.nio.charset.Charset;
import java.util.List;
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
                // logger.trace("EXPORTING METRICS:  {}", sb);

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
            if (e.code() == KeeperException.Code.NONODE) {
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
            if (e.code() == KeeperException.Code.NONODE) {
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
            String dataPath = pathScheme.getAbsolutePath(PathType.DATA,
                    pathScheme.joinTokens(clusterId, serviceId, nodeId));
            byte[] bytes = getContext().getZkClient().getData(dataPath, false, new Stat());
            MetricsData metricsData = JacksonUtil.getObjectMapper().readValue(bytes, MetricsData.class);
            return metricsData;
        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NONODE) {
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
        Runnable aggregationRunnable = new AggregationRunnable();
        Runnable cleanerRunnable = new CleanerRunnable();
        executorService.scheduleAtFixedRate(aggregationRunnable, this.updateIntervalMillis / 2,
                this.updateIntervalMillis, TimeUnit.MILLISECONDS);
        executorService.scheduleAtFixedRate(cleanerRunnable, this.updateIntervalMillis / 2,
                Math.max(this.updateIntervalMillis * 2, 60000), TimeUnit.MILLISECONDS);
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

        long millisLeft(MetricsData metricsData) {
            if (metricsData == null) {
                return -1;
            }
            long currentTimestamp = System.currentTimeMillis();
            long intervalLengthMillis = metricsData.getIntervalLengthTimeUnit().toMillis(
                    metricsData.getIntervalLength());
            return metricsData.getIntervalStartTimestamp() + intervalLengthMillis + 30000 - currentTimestamp;
        }

        @Override
        public void run() {
            PresenceService presenceService = getContext().getService("presence");
            CoordinationService coordinationService = getContext().getService("coord");
            ZkClient zkClient = getContext().getZkClient();
            PathScheme pathScheme = getContext().getPathScheme();

            // list all services in cluster
            List<String> clusterIds = presenceService.lookupClusters();
            for (String clusterId : clusterIds) {
                List<String> serviceIds = presenceService.lookupServices(clusterId);
                for (String serviceId : serviceIds) {
                    logger.trace("Checking data nodes:  clusterId={}; serviceId={}", clusterId, serviceId);

                    // get lock for a service -- should only perform if in cluster
                    DistributedLock cleanerLock = coordinationService.getLock("reign", "metrics-cleaner-" + serviceId);
                    try {
                        if (cleanerLock.tryLock()) {
                            // get all data nodes for a service
                            String dataParentPath = pathScheme.getAbsolutePath(PathType.DATA,
                                    pathScheme.joinTokens(clusterId, serviceId));
                            List<String> dataNodes = zkClient.getChildren(dataParentPath, false);

                            // remove all nodes that are older than rotation interval
                            for (String dataNode : dataNodes) {
                                logger.trace("Checking data nodes:  clusterId={}; serviceId={}; nodeId={}", clusterId,
                                        serviceId, dataNode);
                                String dataPath = pathScheme.getAbsolutePath(PathType.DATA,
                                        pathScheme.joinTokens(clusterId, serviceId, dataNode));
                                MetricsData metricsData = getMetrics(clusterId, serviceId, dataNode);
                                long millisLeft = millisLeft(metricsData);
                                if (millisLeft <= 0) {
                                    logger.info("Removing expired data node:  path={}; millisLeft={}", dataPath,
                                            millisLeft);
                                    zkClient.delete(dataPath, -1);
                                } else {
                                    logger.trace("Data node is not yet expired:  path={}; millisLeft={}", dataPath,
                                            millisLeft);
                                }
                            }
                        }
                    } catch (KeeperException e) {
                        if (e.code() != KeeperException.Code.NONODE) {
                            logger.warn("Error trying to clean up data directory for service:  clusterId=" + clusterId
                                    + "; serviceId=" + serviceId + ":  " + e, e);
                        }
                    } catch (Exception e) {
                        logger.warn("Error trying to clean up data directory for service:  clusterId=" + clusterId
                                + "; serviceId=" + serviceId + ":  " + e, e);
                    } finally {
                        cleanerLock.unlock();
                        cleanerLock.destroy();
                    }// try
                }// for service
            }// for cluster
        }// run()
    }

}
