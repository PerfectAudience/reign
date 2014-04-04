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
import io.reign.ReignException;
import io.reign.ZkClient;
import io.reign.coord.CoordinationService;
import io.reign.coord.DistributedLock;
import io.reign.presence.PresenceService;
import io.reign.util.JacksonUtil;
import io.reign.util.ZkClientUtil;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
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

    public static final int DEFAULT_UPDATE_INTERVAL_MILLIS = 10000;

    /** interval btw. aggregations at service level */
    private volatile int updateIntervalMillis = DEFAULT_UPDATE_INTERVAL_MILLIS;

    private final Map<String, ExportMeta> exportPathMap = new ConcurrentHashMap<String, ExportMeta>(16, 0.9f, 1);

    private static class ExportMeta {
        public String dataPath;
        public MetricRegistryManager registryManager;

        public ExportMeta(String dataPath, MetricRegistryManager registryManager) {
            this.dataPath = dataPath;
            this.registryManager = registryManager;
        }
    }

    private final ZkClientUtil zkClientUtil = new ZkClientUtil();

    private ScheduledExecutorService executorService;
    private volatile ScheduledFuture aggregationFuture;

    public void observe(final String clusterId, final String serviceId, MetricsObserver observer) {
        String servicePath = getPathScheme().joinTokens(clusterId, serviceId);
        String path = getPathScheme().getAbsolutePath(PathType.METRICS, servicePath);
        getObserverManager().put(path, observer);
    }

    public MetricRegistryManager getRegistered(String clusterId, String serviceId) {
        String key = exportPathMapKey(clusterId, serviceId, getContext().getZkNodeId().getPathToken());
        synchronized (this) {
            ExportMeta exportMeta = exportPathMap.get(key);
            if (exportMeta != null) {
                return exportMeta.registryManager;
            } else {
                return null;
            }
        }
    }

    /**
     * Registers metrics for export to ZK.
     */
    public void register(final String clusterId, final String serviceId, final MetricRegistryManager registryManager,
            long updateInterval, TimeUnit updateIntervalTimeUnit) {
        register(clusterId, serviceId, getContext().getZkNodeId().getPathToken(), registryManager, updateInterval,
                updateIntervalTimeUnit);
    }

    String exportPathMapKey(String clusterId, String serviceId, String nodeId) {
        return clusterId + "/" + serviceId + "/" + nodeId;
    }

    void register(final String clusterId, final String serviceId, final String nodeId,
            final MetricRegistryManager registryManager, long updateInterval, TimeUnit updateIntervalTimeUnit) {

        final String key = exportPathMapKey(clusterId, serviceId, nodeId);
        synchronized (this) {
            if (!exportPathMap.containsKey(key)) {
                exportPathMap.put(key, new ExportMeta(null, registryManager));
            } else {
                logger.info("Already exported metrics:  {}", key);
                return;
            }
        }

        final ZkMetricsReporter reporter = ZkMetricsReporter.forRegistry(registryManager)
                .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build();

        executorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                // logger.trace("EXPORTING METRICS...");
                StringBuilder sb = new StringBuilder();
                sb = reporter.report(sb);
                // logger.trace("EXPORTING METRICS:  clusterId={}; serviceId={}; data=\n{}", clusterId, serviceId, sb);

                // export to zk
                ExportMeta exportMeta = exportPathMap.get(key);
                try {
                    if (exportMeta.dataPath == null) {
                        PathScheme pathScheme = getContext().getPathScheme();
                        String dataPathPrefix = pathScheme.getAbsolutePath(PathType.METRICS,
                                pathScheme.joinTokens(clusterId, serviceId, nodeId));
                        exportMeta.dataPath = zkClientUtil.updatePath(getContext().getZkClient(), getContext()
                                .getPathScheme(), dataPathPrefix + "-", sb.toString().getBytes(UTF_8), getContext()
                                .getDefaultZkAclList(), CreateMode.PERSISTENT_SEQUENTIAL, -1);

                        // put in again to update data
                        exportPathMap.put(key, exportMeta);

                        synchronized (exportMeta) {
                            exportMeta.notifyAll();
                        }

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
                registryManager.rotateAsNecessary();

            }
        }, 0, updateInterval, updateIntervalTimeUnit);
    }

    /**
     * Get metrics data for given service.
     */
    public MetricsData getServiceMetrics(String clusterId, String serviceId) {
        try {
            PathScheme pathScheme = getContext().getPathScheme();
            String dataPath = pathScheme.getAbsolutePath(PathType.METRICS, pathScheme.joinTokens(clusterId, serviceId));
            byte[] bytes = getContext().getZkClient().getData(dataPath, true, new Stat());
            // String metricsDataJson = new String(bytes, UTF_8);
            // metricsDataJson.replaceAll("\n", "");
            MetricsData metricsData = null;
            if (bytes != null) {
                metricsData = JacksonUtil.getObjectMapper().readValue(bytes, MetricsData.class);
            }
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

    /**
     * Get metrics data for this service node (self).
     */
    public MetricsData getMetrics(String clusterId, String serviceId) {
        String key = clusterId + "/" + serviceId + "/" + getContext().getZkNodeId().getPathToken();
        ExportMeta exportMeta = exportPathMap.get(key);
        if (exportMeta == null) {
            logger.trace(
                    "MetricsData not found:  data has not been exported:  clusterId={}; serviceId={}; exportMeta={}",
                    clusterId, serviceId, exportMeta);
            return null;
        }
        if (exportMeta.dataPath == null) {
            logger.trace(
                    "MetricsData not found:  waiting for data to be reported in ZK:  clusterId={}; serviceId={}; exportMeta.dataPath={}",
                    clusterId, serviceId, exportMeta.dataPath);
            synchronized (exportMeta) {
                try {
                    exportMeta.wait();
                } catch (InterruptedException e) {
                    logger.warn("Interrupted while waiting:  " + e, e);
                }
            }
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

    MetricsData getMetricsFromDataNode(String clusterId, String serviceId, String dataNode) {
        try {
            PathScheme pathScheme = getContext().getPathScheme();
            String dataPath = pathScheme.getAbsolutePath(PathType.METRICS,
                    pathScheme.joinTokens(clusterId, serviceId, dataNode));
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
        if (updateIntervalMillis < 1000) {
            throw new ReignException("updateIntervalMillis is too short:  updateIntervalMillis=" + updateIntervalMillis);
        }
        this.updateIntervalMillis = updateIntervalMillis;
        scheduleAggregation();
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
        scheduleAggregation();
        scheduleCleaner();

    }

    public synchronized void scheduleCleaner() {
        Runnable cleanerRunnable = new CleanerRunnable();
        executorService.scheduleAtFixedRate(cleanerRunnable, this.updateIntervalMillis / 2,
                Math.max(this.updateIntervalMillis * 2, 60000), TimeUnit.MILLISECONDS);
    }

    public synchronized void scheduleAggregation() {
        if (aggregationFuture != null) {
            aggregationFuture.cancel(false);
        }
        Runnable aggregationRunnable = new AggregationRunnable();
        aggregationFuture = executorService.scheduleAtFixedRate(aggregationRunnable,
                Math.min(this.updateIntervalMillis / 2, 1000), this.updateIntervalMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void destroy() {
        executorService.shutdown();
    }

    long millisToExpiry(MetricsData metricsData) {
        if (metricsData == null) {
            return -1;
        }
        long currentTimestamp = System.currentTimeMillis();
        long intervalLengthMillis = metricsData.getIntervalLengthTimeUnit().toMillis(metricsData.getIntervalLength());
        return metricsData.getIntervalStartTimestamp() + intervalLengthMillis + 30000 - currentTimestamp;
    }

    public class AggregationRunnable implements Runnable {
        @Override
        public void run() {
            logger.trace("AggregationRunnable starting:  hashCode={}", this.hashCode());

            // list all services in cluster
            PresenceService presenceService = getContext().getService("presence");
            CoordinationService coordinationService = getContext().getService("coord");
            ZkClient zkClient = getContext().getZkClient();
            PathScheme pathScheme = getContext().getPathScheme();

            // list all services in cluster
            List<String> clusterIds = presenceService.getClusters();
            for (String clusterId : clusterIds) {

                // only proceed if in cluster
                if (!presenceService.isMemberOf(clusterId)) {
                    continue;
                }

                List<String> serviceIds = presenceService.getServices(clusterId);
                for (String serviceId : serviceIds) {
                    logger.trace("Finding data nodes:  clusterId={}; serviceId={}", clusterId, serviceId);

                    // only proceed if in service
                    if (!presenceService.isMemberOf(clusterId, serviceId)) {
                        continue;
                    }

                    // get lock for a service
                    DistributedLock lock = coordinationService.getLock("reign", "metrics-aggregator-" + clusterId + "-"
                            + serviceId);
                    try {
                        if (lock.tryLock()) {
                            // get all data nodes for a service
                            String dataParentPath = pathScheme.getAbsolutePath(PathType.METRICS,
                                    pathScheme.joinTokens(clusterId, serviceId));
                            List<String> dataNodes = zkClient.getChildren(dataParentPath, false);

                            /** iterate through service data nodes and gather up data to aggregate **/
                            Map<String, List<CounterData>> counterMap = new HashMap<String, List<CounterData>>(
                                    dataNodes.size() + 1, 1.0f);
                            Map<String, List<GaugeData>> gaugeMap = new HashMap<String, List<GaugeData>>(
                                    dataNodes.size() + 1, 1.0f);
                            Map<String, List<HistogramData>> histogramMap = new HashMap<String, List<HistogramData>>(
                                    dataNodes.size() + 1, 1.0f);
                            Map<String, List<MeterData>> meterMap = new HashMap<String, List<MeterData>>(
                                    dataNodes.size() + 1, 1.0f);
                            Map<String, List<TimerData>> timerMap = new HashMap<String, List<TimerData>>(
                                    dataNodes.size() + 1, 1.0f);
                            int nodeCount = 0;
                            for (String dataNode : dataNodes) {
                                logger.trace("Found data node:  clusterId={}; serviceId={}; nodeId={}", clusterId,
                                        serviceId, dataNode);
                                String dataPath = pathScheme.getAbsolutePath(PathType.METRICS,
                                        pathScheme.joinTokens(clusterId, serviceId, dataNode));
                                MetricsData metricsData = getMetricsFromDataNode(clusterId, serviceId, dataNode);

                                // skip data node if not within interval
                                long millisToExpiry = millisToExpiry(metricsData);
                                if (millisToExpiry <= 0) {
                                    continue;

                                }

                                // aggregate service stats for data nodes that within current rotation interval
                                logger.trace("Aggregating data node:  path={}; millisToExpiry={}", dataPath,
                                        millisToExpiry);

                                // increment node count
                                nodeCount++;

                                // counters
                                Map<String, CounterData> counters = metricsData.getCounters();
                                for (String key : counters.keySet()) {
                                    CounterData counter = counters.get(key);
                                    List<CounterData> counterList = counterMap.get(key);
                                    if (counterList == null) {
                                        counterList = new ArrayList<CounterData>(dataNodes.size());
                                        counterMap.put(key, counterList);
                                    }
                                    counterList.add(counter);
                                }

                                // gauges
                                Map<String, GaugeData> gauges = metricsData.getGauges();
                                for (String key : gauges.keySet()) {
                                    GaugeData gauge = gauges.get(key);
                                    List<GaugeData> gaugeList = gaugeMap.get(key);
                                    if (gaugeList == null) {
                                        gaugeList = new ArrayList<GaugeData>(dataNodes.size());
                                        gaugeMap.put(key, gaugeList);
                                    }
                                    gaugeList.add(gauge);
                                }

                                // histogram
                                Map<String, HistogramData> histograms = metricsData.getHistograms();
                                for (String key : histograms.keySet()) {
                                    HistogramData histogram = histograms.get(key);
                                    List<HistogramData> histogramList = histogramMap.get(key);
                                    if (histogramList == null) {
                                        histogramList = new ArrayList<HistogramData>(dataNodes.size());
                                        histogramMap.put(key, histogramList);
                                    }
                                    histogramList.add(histogram);
                                }

                                // meters
                                Map<String, MeterData> meters = metricsData.getMeters();
                                for (String key : meters.keySet()) {
                                    MeterData meter = meters.get(key);
                                    List<MeterData> meterList = meterMap.get(key);
                                    if (meterList == null) {
                                        meterList = new ArrayList<MeterData>(dataNodes.size());
                                        meterMap.put(key, meterList);
                                    }
                                    meterList.add(meter);
                                }

                                // timers
                                Map<String, TimerData> timers = metricsData.getTimers();
                                for (String key : timers.keySet()) {
                                    TimerData timer = timers.get(key);
                                    List<TimerData> meterList = timerMap.get(key);
                                    if (meterList == null) {
                                        meterList = new ArrayList<TimerData>(dataNodes.size());
                                        timerMap.put(key, meterList);
                                    }
                                    meterList.add(timer);
                                }

                            }// for dataNodes

                            /** aggregate data and write to ZK **/
                            MetricsData serviceMetricsData = new MetricsData();

                            // counters
                            Map<String, CounterData> counters = new HashMap<String, CounterData>(counterMap.size() + 1,
                                    1.0f);
                            for (String key : counterMap.keySet()) {
                                List<CounterData> counterList = counterMap.get(key);
                                if (counterList.size() != nodeCount) {
                                    logger.warn(
                                            "counterList size does not match nodeCount:  counterList.size={}; nodeCount={}",
                                            counterList.size(), nodeCount);
                                }
                                CounterData counterData = CounterData.merge(counterList);
                                counters.put(key, counterData);
                            }
                            serviceMetricsData.setCounters(counters);

                            // gauges
                            Map<String, GaugeData> gauges = new HashMap<String, GaugeData>(gaugeMap.size() + 1, 1.0f);
                            for (String key : gaugeMap.keySet()) {
                                List<GaugeData> gaugeList = gaugeMap.get(key);
                                if (gaugeList.size() != nodeCount) {
                                    logger.warn(
                                            "gaugeList size does not match nodeCount:  gaugeList.size={}; nodeCount={}",
                                            gaugeList.size(), nodeCount);
                                }
                                GaugeData gaugeData = GaugeData.merge(gaugeList);
                                gauges.put(key, gaugeData);
                            }
                            serviceMetricsData.setGauges(gauges);

                            // histograms
                            Map<String, HistogramData> histograms = new HashMap<String, HistogramData>(
                                    histogramMap.size() + 1, 1.0f);
                            for (String key : histogramMap.keySet()) {
                                List<HistogramData> histogramList = histogramMap.get(key);
                                if (histogramList.size() != nodeCount) {
                                    logger.warn(
                                            "histogramList size does not match nodeCount:  histogramList.size={}; nodeCount={}",
                                            histogramList.size(), nodeCount);
                                }
                                HistogramData histogramData = HistogramData.merge(histogramList);
                                histograms.put(key, histogramData);
                            }
                            serviceMetricsData.setHistograms(histograms);

                            // meters
                            Map<String, MeterData> meters = new HashMap<String, MeterData>(meterMap.size() + 1, 1.0f);
                            for (String key : meterMap.keySet()) {
                                List<MeterData> meterList = meterMap.get(key);
                                if (meterList.size() != nodeCount) {
                                    logger.warn(
                                            "meterList size does not match nodeCount:  meterList.size={}; nodeCount={}",
                                            meterList.size(), nodeCount);
                                }
                                MeterData meterData = MeterData.merge(meterList);
                                meters.put(key, meterData);
                            }
                            serviceMetricsData.setMeters(meters);

                            // timers
                            Map<String, TimerData> timers = new HashMap<String, TimerData>(timerMap.size() + 1, 1.0f);
                            for (String key : timerMap.keySet()) {
                                List<TimerData> timerList = timerMap.get(key);
                                if (timerList.size() != nodeCount) {
                                    logger.warn(
                                            "timerList size does not match nodeCount:  timerList.size={}; nodeCount={}",
                                            timerList.size(), nodeCount);
                                }
                                TimerData timerData = TimerData.merge(timerList);
                                timers.put(key, timerData);
                            }
                            serviceMetricsData.setTimers(timers);

                            serviceMetricsData.setNodeCount(nodeCount);

                            // write to ZK
                            String dataPath = pathScheme.getAbsolutePath(PathType.METRICS,
                                    pathScheme.joinTokens(clusterId, serviceId));
                            String serviceMetricsDataString = JacksonUtil.getObjectMapper().writeValueAsString(
                                    serviceMetricsData);
                            // logger.trace("EXPORT SERVICE DATA:  {}", serviceMetricsDataString);
                            zkClientUtil.updatePath(getContext().getZkClient(), getContext().getPathScheme(), dataPath,
                                    serviceMetricsDataString.getBytes(UTF_8), getContext().getDefaultZkAclList(),
                                    CreateMode.PERSISTENT, -1);

                        }// if try lock
                    } catch (KeeperException e) {
                        if (e.code() != KeeperException.Code.NONODE) {
                            logger.warn("Error trying to clean up data directory for service:  clusterId=" + clusterId
                                    + "; serviceId=" + serviceId + ":  " + e, e);
                        }
                    } catch (Exception e) {
                        logger.warn("Error trying to clean up data directory for service:  clusterId=" + clusterId
                                + "; serviceId=" + serviceId + ":  " + e, e);
                    } finally {
                        lock.unlock();
                        lock.destroy();
                    }// try

                }// for service

                // store aggregated results in ZK at service level
            }// for cluster

        }// run
    }

    public class CleanerRunnable implements Runnable {

        @Override
        public void run() {
            logger.trace("CleanerRunnable starting:  hashCode={}", this.hashCode());

            PresenceService presenceService = getContext().getService("presence");
            CoordinationService coordinationService = getContext().getService("coord");
            ZkClient zkClient = getContext().getZkClient();
            PathScheme pathScheme = getContext().getPathScheme();

            // list all services in cluster
            List<String> clusterIds = presenceService.getClusters();
            for (String clusterId : clusterIds) {

                // only proceed if in cluster
                if (!presenceService.isMemberOf(clusterId)) {
                    continue;
                }

                List<String> serviceIds = presenceService.getServices(clusterId);
                for (String serviceId : serviceIds) {
                    logger.trace("Checking data nodes expiry:  clusterId={}; serviceId={}", clusterId, serviceId);

                    // only proceed if in service
                    if (!presenceService.isMemberOf(clusterId, serviceId)) {
                        continue;
                    }

                    // get lock for a service
                    DistributedLock lock = coordinationService.getLock("reign", "metrics-cleaner-" + clusterId + "-"
                            + serviceId);
                    String dataPath = null;
                    try {
                        if (lock.tryLock()) {
                            // get all data nodes for a service
                            String dataParentPath = pathScheme.getAbsolutePath(PathType.METRICS,
                                    pathScheme.joinTokens(clusterId, serviceId));
                            List<String> dataNodes = zkClient.getChildren(dataParentPath, false);

                            // remove all nodes that are older than rotation interval
                            for (String dataNode : dataNodes) {
                                logger.trace("Checking data node expiry:  clusterId={}; serviceId={}; nodeId={}",
                                        clusterId, serviceId, dataNode);
                                dataPath = pathScheme.getAbsolutePath(PathType.METRICS,
                                        pathScheme.joinTokens(clusterId, serviceId, dataNode));
                                MetricsData metricsData = getMetricsFromDataNode(clusterId, serviceId, dataNode);
                                long millisToExpiry = millisToExpiry(metricsData);
                                if (millisToExpiry <= 0) {
                                    logger.info("Removing expired data node:  path={}; millisToExpiry={}", dataPath,
                                            millisToExpiry);
                                    zkClient.delete(dataPath, -1);
                                } else {
                                    logger.trace("Data node is not yet expired:  path={}; millisToExpiry={}", dataPath,
                                            millisToExpiry);
                                }
                            }
                        }
                    } catch (KeeperException e) {
                        if (e.code() != KeeperException.Code.NONODE) {
                            logger.warn("Error trying to clean up data directory for service:  clusterId=" + clusterId
                                    + "; serviceId=" + serviceId + "; dataPath=" + dataPath + ":  " + e, e);
                        }
                    } catch (Exception e) {
                        logger.warn("Error trying to clean up data directory for service:  clusterId=" + clusterId
                                + "; serviceId=" + serviceId + "; dataPath=" + dataPath + ":  " + e, e);
                    } finally {
                        lock.unlock();
                        lock.destroy();
                    }// try
                }// for service
            }// for cluster
        }// run()
    }

}
