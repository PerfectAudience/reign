/*
 * Copyright 2013 Yen Pai ypai@reign.io
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

import io.reign.AbstractService;
import io.reign.NodeAddress;
import io.reign.PathScheme;
import io.reign.PathType;
import io.reign.Reign;
import io.reign.ReignException;
import io.reign.ZkClient;
import io.reign.coord.CoordinationService;
import io.reign.coord.DistributedLock;
import io.reign.mesg.MessagingService;
import io.reign.mesg.ParsedRequestMessage;
import io.reign.mesg.RequestMessage;
import io.reign.mesg.ResponseMessage;
import io.reign.mesg.ResponseStatus;
import io.reign.mesg.SimpleEventMessage;
import io.reign.mesg.SimpleResponseMessage;
import io.reign.presence.PresenceService;
import io.reign.util.JacksonUtil;
import io.reign.util.ZkClientUtil;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.map.exc.UnrecognizedPropertyException;
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

	/** interval btw. aggregations at service level */
	private volatile int updateIntervalMillis = DEFAULT_UPDATE_INTERVAL_MILLIS;

	private final Map<String, ExportMeta> exportPathMap = new ConcurrentHashMap<String, ExportMeta>(16, 0.9f, 1);

	private static class ExportMeta {
		public volatile String dataPath;
		public volatile MetricRegistryManager registryManager;
		public volatile Future future = null;
		public volatile ZkMetricsReporter metricsReporter;

		public ExportMeta(String dataPath, MetricRegistryManager registryManager, ZkMetricsReporter metricsReporter) {
			this.dataPath = dataPath;
			this.registryManager = registryManager;
			this.metricsReporter = metricsReporter;
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
		String key = exportPathMapKey(clusterId, serviceId, getContext().getNodeId());
		synchronized (exportPathMap) {
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
	public void scheduleExport(final String clusterId, final String serviceId,
	        final MetricRegistryManager registryManager, long updateInterval, TimeUnit updateIntervalTimeUnit) {
		scheduleExport(clusterId, serviceId, getContext().getNodeId(), registryManager, updateInterval,
		        updateIntervalTimeUnit);
	}

	String exportPathMapKey(String clusterId, String serviceId, String nodeId) {
		return clusterId + "/" + serviceId + "/" + nodeId;
	}

	void scheduleExport(final String clusterId, final String serviceId, final String nodeId,
	        final MetricRegistryManager registryManager, long updateInterval, TimeUnit updateIntervalTimeUnit) {

		final String key = exportPathMapKey(clusterId, serviceId, nodeId);
		synchronized (exportPathMap) {
			if (!exportPathMap.containsKey(key)) {
				exportPathMap.put(key, new ExportMeta(null, registryManager, null));
			} else {
				logger.info("Metrics export already scheduled:  {}", key);
				return;
			}
		}

		final ZkMetricsReporter reporter = ZkMetricsReporter.builder().convertRatesTo(TimeUnit.SECONDS)
		        .convertDurationsTo(TimeUnit.MILLISECONDS).build();

		// determine runnable interval
		long updateIntervalSeconds = updateIntervalTimeUnit.toSeconds(updateInterval);
		updateIntervalSeconds = Math.min(updateIntervalSeconds / 2,
		        registryManager.getRotationTimeUnit().toSeconds(registryManager.getRotationInterval()) / 2);
		if (updateIntervalSeconds < 1) {
			updateIntervalSeconds = 1;
		}

		// get export metadata for this key
		final ExportMeta exportMeta = exportPathMap.get(key);
		if (exportMeta.future != null) {
			// cancel existing job if there is one
			exportMeta.future.cancel(false);
		}

		exportMeta.metricsReporter = reporter;

		// create future
		exportMeta.future = executorService.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				// export to zk
				try {
					synchronized (exportMeta) {
						MetricRegistry currentMetricRegistry = registryManager.get();

						// logger.trace("EXPORTING METRICS...");
						StringBuilder sb = new StringBuilder();
						sb = reporter.report(currentMetricRegistry, registryManager.getLastRotatedTimestamp(),
						        registryManager.getRotationInterval(), registryManager.getRotationTimeUnit(), sb);
						// logger.trace("EXPORTING METRICS:  clusterId={}; serviceId={}; data=\n{}",
						// clusterId,
						// serviceId,
						// sb);

						if (exportMeta.dataPath == null) {
							PathScheme pathScheme = getContext().getPathScheme();
							String dataPathPrefix = pathScheme.getAbsolutePath(PathType.METRICS,
							        pathScheme.joinTokens(clusterId, serviceId, nodeId)).replace("\"", "'");

							// update node and get new path
							exportMeta.dataPath = zkClientUtil.updatePath(getContext().getZkClient(), getContext()
							        .getPathScheme(), dataPathPrefix + "-", sb.toString().getBytes(UTF_8), getContext()
							        .getDefaultZkAclList(), CreateMode.PERSISTENT_SEQUENTIAL, -1);

							logger.debug("New data path:  path={}", exportMeta.dataPath);

							// put in again to update data
							exportPathMap.put(key, exportMeta);

						} else {
							logger.debug("Updating data path:  path={}", exportMeta.dataPath);

							// update node
							zkClientUtil.updatePath(getContext().getZkClient(), getContext().getPathScheme(),
							        exportMeta.dataPath, sb.toString().getBytes(UTF_8), getContext()
							                .getDefaultZkAclList(), CreateMode.PERSISTENT, -1);
						}

						exportMeta.notifyAll();
					}

					logger.debug("Updated metrics data:  dataPath={}", exportMeta.dataPath);

				} catch (Exception e) {
					logger.error("Could not export metrics data:  clusterId=" + clusterId + "; serviceId=" + serviceId
					        + "; nodeId=" + nodeId + "; dataPath=" + exportMeta.dataPath, e);
				}

			}
		}, 0, updateIntervalSeconds, TimeUnit.SECONDS);
	}

	/**
	 * Get metrics data for given service.
	 */
	public MetricsData getServiceMetrics(String clusterId, String serviceId) {
		MetricsData metricsData = getMetricsFromDataNode(clusterId, serviceId, null);
		return metricsData;
	}

	/**
	 * Get metrics data for this service node (self) for current interval.
	 */
	public MetricsData getMyMetrics(String clusterId, String serviceId) {
		String key = clusterId + "/" + serviceId + "/" + getContext().getNodeId();
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
			Stat stat = new Stat();
			byte[] bytes = getContext().getZkClient().getData(exportMeta.dataPath, true, stat);
			MetricsData metricsData = JacksonUtil.getObjectMapper().readValue(bytes, MetricsData.class);
			metricsData.setClusterId(clusterId);
			metricsData.setServiceId(serviceId);
			metricsData.setLastUpdatedTimestamp(stat.getMtime());
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
		PathScheme pathScheme = getContext().getPathScheme();
		String dataPath = null;
		if (dataNode != null) {
			dataPath = pathScheme.getAbsolutePath(PathType.METRICS,
			        pathScheme.joinTokens(clusterId, serviceId, dataNode));
		} else {
			dataPath = pathScheme.getAbsolutePath(PathType.METRICS, pathScheme.joinTokens(clusterId, serviceId));
		}
		byte[] bytes = null;
		try {
			Stat stat = new Stat();
			bytes = getContext().getZkClient().getData(dataPath, true, stat);
			MetricsData metricsData = JacksonUtil.getObjectMapper().readValue(bytes, MetricsData.class);
			metricsData.setLastUpdatedTimestamp(stat.getMtime());
			return metricsData;
		} catch (KeeperException e) {
			if (e.code() == KeeperException.Code.NONODE) {
				return null;
			}
			logger.warn("Error retrieving data node:  clusterId=" + clusterId + "; serviceId=" + serviceId
			        + "; dataPath=" + dataPath + "; dataAsString=" + (new String(bytes, UTF_8)) + ":  " + e, e);
			throw new ReignException("Error retrieving data node:  clusterId=" + clusterId + "; serviceId=" + serviceId
			        + "; dataPath=" + dataPath + "; dataAsString=" + (new String(bytes, UTF_8)), e);
		} catch (UnrecognizedPropertyException e) {
			logger.warn("Error retrieving data node:  clusterId=" + clusterId + "; serviceId=" + serviceId
			        + "; dataPath=" + dataPath + "; dataAsString=" + (new String(bytes, UTF_8)) + ":  " + e, e);
			return null;
		} catch (Exception e) {
			logger.warn("Error retrieving data node:  clusterId=" + clusterId + "; serviceId=" + serviceId
			        + "; dataPath=" + dataPath + "; dataAsString=" + (new String(bytes, UTF_8)) + ":  " + e, e);
			throw new ReignException("Error retrieving data node:  clusterId=" + clusterId + "; serviceId=" + serviceId
			        + "; dataPath=" + dataPath + "; dataAsString=" + (new String(bytes, UTF_8)), e);
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
		scheduleRotator();

	}

	synchronized void scheduleRotator() {
		Runnable cleanerRunnable = new RotatorRunnable();
		executorService.scheduleAtFixedRate(cleanerRunnable, 500, 500, TimeUnit.MILLISECONDS);
	}

	class RotatorRunnable implements Runnable {
		@Override
		public void run() {
			try {
				for (Entry<String, ExportMeta> mapEntry : exportPathMap.entrySet()) {
					ExportMeta exportMeta = mapEntry.getValue();

					// rotate as necessary
					synchronized (exportMeta) {
						MetricRegistryManager registryManager = exportMeta.registryManager;
						if (registryManager == null) {
							continue;
						}

						long oldLastRotatedTimestamp = registryManager.getLastRotatedTimestamp();
						MetricRegistry currentMetricRegistry = registryManager.rotateAsNecessary();
						MetricRegistry workingMetricRegistry = registryManager.get();
						if (currentMetricRegistry != null && currentMetricRegistry != workingMetricRegistry) {

							if (exportMeta.dataPath != null) {

								try {
									// write out stats for old metric registry
									StringBuilder sb = new StringBuilder();
									sb = exportMeta.metricsReporter.report(currentMetricRegistry,
									        oldLastRotatedTimestamp, registryManager.getRotationInterval(),
									        registryManager.getRotationTimeUnit(), sb);

									if (logger.isDebugEnabled()) {
										logger.debug(
										        "Flushing to old data node after rotation:  currentMetricRegistry={}; workingMetricRegistry={}; path={}; data={}",
										        currentMetricRegistry, workingMetricRegistry, exportMeta.dataPath, sb
										                .toString().replace("\n", ""));
									}

									zkClientUtil.updatePath(getContext().getZkClient(), getContext().getPathScheme(),
									        exportMeta.dataPath, sb.toString().getBytes(UTF_8), getContext()
									                .getDefaultZkAclList(), CreateMode.PERSISTENT, -1);
								} catch (Exception e) {
									logger.error("Could not export update metrics data after rotation:  path="
									        + exportMeta.dataPath, e);
								}
							}

							// set to null to force creation of new node in ZK
							exportMeta.dataPath = null;
						}
					}

				}
			} catch (Exception e) {
				logger.error("Unexpected exception:  " + e, e);
			}
		}// run
	}

	synchronized void scheduleCleaner() {
		Runnable cleanerRunnable = new CleanerRunnable();
		executorService.scheduleAtFixedRate(cleanerRunnable, this.updateIntervalMillis / 2,
		        Math.max(this.updateIntervalMillis * 2, 60000), TimeUnit.MILLISECONDS);
	}

	synchronized void scheduleAggregation() {
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

	@Override
	public ResponseMessage handleMessage(RequestMessage requestMessage) {
		ResponseMessage responseMessage = new SimpleResponseMessage();

		try {
			if (logger.isTraceEnabled()) {
				logger.trace("Received message:  nodeId={}; request='{}:{}'", requestMessage.getSenderInfo(),
				        requestMessage.getTargetService(), requestMessage.getBody());
			}

			/** preprocess request **/
			ParsedRequestMessage parsedRequestMessage = new ParsedRequestMessage(requestMessage);
			String resource = parsedRequestMessage.getResource();

			// strip beginning and ending slashes "/"
			boolean endsWithSlash = false;
			if (resource.startsWith("/")) {
				resource = resource.substring(1);
			}
			if (resource.endsWith("/")) {
				endsWithSlash = true;
				resource = resource.substring(0, resource.length() - 1);
			}

			/** get response **/
			if ("observe".equals(parsedRequestMessage.getMeta())) {
				responseMessage = new SimpleResponseMessage(ResponseStatus.OK);
				String[] tokens = getPathScheme().tokenizePath(resource);
				if (tokens.length == 2) {
					this.observe(tokens[0], tokens[1],
					        this.getClientObserver(parsedRequestMessage.getSenderInfo(), tokens[0], tokens[1], null));
				} else if (tokens.length == 3) {
					this.observe(tokens[0], tokens[1], this.getClientObserver(parsedRequestMessage.getSenderInfo(),
					        tokens[0], tokens[1], tokens[2]));
				} else {
					responseMessage.setComment("Observing not supported:  " + resource);
				}
			} else if ("observe-stop".equals(parsedRequestMessage.getMeta())) {
				responseMessage = new SimpleResponseMessage(ResponseStatus.OK);
				String absolutePath = getPathScheme().getAbsolutePath(PathType.METRICS, resource);
				getContext().getObserverManager().removeByOwnerId(parsedRequestMessage.getSenderInfo().toString(),
				        absolutePath);
			} else {
				if (resource.length() == 0) {
					// list available clusters
					String path = getContext().getPathScheme().getAbsolutePath(PathType.METRICS);
					List<String> clusterList = getContext().getZkClient().getChildren(path, false);
					responseMessage.setBody(clusterList);

				} else {
					String[] tokens = getPathScheme().tokenizePath(resource);
					// logger.debug("tokens.length={}", tokens.length);

					if (tokens.length == 1) {
						// list available services
						String path = getContext().getPathScheme().getAbsolutePath(PathType.METRICS, tokens[0]);
						List<String> serviceList = getContext().getZkClient().getChildren(path, false);
						responseMessage.setBody(serviceList);
						if (serviceList == null) {
							responseMessage.setComment("Not found:  " + resource);
						}

					} else if (tokens.length == 2) {
						if (endsWithSlash) {
							// list available nodes for a given service
							String path = getContext().getPathScheme().getAbsolutePath(PathType.METRICS, tokens[0],
							        tokens[1]);
							List<String> nodeList = getContext().getZkClient().getChildren(path, false);

							responseMessage.setBody(nodeList);
							if (nodeList == null) {
								responseMessage.setComment("Not found:  " + resource);
							}
						} else {
							// get metrics data for service
							MetricsData metricsData = getMetricsFromDataNode(tokens[0], tokens[1], null);
							if (metricsData == null) {
								responseMessage.setComment("Not found:  " + resource);
							} else {
								responseMessage.setBody(metricsData);
							}
						}

					} else if (tokens.length == 3) {
						// get metrics data for single data node
						MetricsData metricsData = getMetricsFromDataNode(tokens[0], tokens[1], tokens[2]);
						if (metricsData == null) {
							responseMessage.setComment("Not found:  " + resource);
						} else {
							responseMessage.setBody(metricsData);
						}

					}
				}
			} // if observe

		} catch (KeeperException e) {
			if (e.code() == KeeperException.Code.NONODE) {
				responseMessage.setBody(Collections.EMPTY_LIST);
			} else {
				responseMessage.setStatus(ResponseStatus.ERROR_UNEXPECTED, "" + e);
			}

		} catch (Exception e) {
			logger.error("" + e, e);
			responseMessage.setStatus(ResponseStatus.ERROR_UNEXPECTED, "" + e);

		}

		responseMessage.setId(requestMessage.getId());

		return responseMessage;

	}

	MetricsObserver getClientObserver(final NodeAddress clientNodeInfo, final String clusterId, final String serviceId,
	        final String nodeId) {
		MetricsObserver observer = new MetricsObserver() {
			@Override
			public void updated(MetricsData updated, MetricsData previous) {

				try {
					Map<String, MetricsData> body = new HashMap<String, MetricsData>(3, 1.0f);
					body.put("updated", updated);
					body.put("previous", previous);

					SimpleEventMessage eventMessage = new SimpleEventMessage();
					eventMessage.setEvent("metrics").setClusterId(clusterId).setServiceId(serviceId).setNodeId(nodeId)
					        .setBody(body);

					MessagingService messagingService = getContext().getService("mesg");
					messagingService.sendMessageFF(getContext().getPathScheme().getFrameworkClusterId(),
					        Reign.CLIENT_SERVICE_ID, clientNodeInfo, eventMessage);
				} catch (Exception e) {
					logger.warn("Trouble notifying client observer:  " + e, e);
				}

			}
		};

		observer.setOwnerId(clientNodeInfo.getNodeId());

		return observer;
	}

	long millisToExpiry(MetricsData metricsData, long currentTimestamp) {
		if (metricsData == null || metricsData.getIntervalLengthUnit() == null
		        || metricsData.getIntervalLength() == null) {
			return -1;
		}
		long intervalLengthMillis = metricsData.getIntervalLengthUnit().toMillis(metricsData.getIntervalLength());
		return metricsData.getIntervalStartTimestamp() + intervalLengthMillis - currentTimestamp;
	}

	public class AggregationRunnable implements Runnable {
		@Override
		public void run() {
			long startTimeNanos = System.nanoTime();

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
				if (!presenceService.isMemberOf(clusterId)
				        || clusterId.equals(getContext().getPathScheme().getFrameworkClusterId())) {
					continue;
				}

				List<String> allServiceIds = presenceService.getServices(clusterId);
				List<String> memberServiceIds = new ArrayList<String>(allServiceIds.size());
				for (String serviceId : allServiceIds) {
					// only aggregate if node is in service
					if (presenceService.isMemberOf(clusterId, serviceId)) {
						memberServiceIds.add(serviceId);
					}
				}

				// go through member service list in deterministic order so
				// locks are acquired in the same order across
				// nodes
				Collections.sort(memberServiceIds);
				for (int i = 0; i < memberServiceIds.size(); i++) {
					long currentTimestamp = System.currentTimeMillis();

					String serviceId = memberServiceIds.get(i);

					logger.trace("Finding data nodes:  clusterId={}; serviceId={}", clusterId, serviceId);

					// get lock for a service
					DistributedLock lock = coordinationService.getLock("reign", "metrics-" + clusterId + "-"
					        + serviceId);
					if (!lock.tryLock()) {
						continue;
					}
					try {

						// get all data nodes for a service
						String dataParentPath = pathScheme.getAbsolutePath(PathType.METRICS,
						        pathScheme.joinTokens(clusterId, serviceId));
						List<String> dataNodes = zkClient.getChildren(dataParentPath, false);

						/**
						 * iterate through service data nodes and gather up data to aggregate
						 **/
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
						int dataNodeCount = 0;
						int dataNodeInWindowCount = 0;
						Integer intervalLength = null;
						TimeUnit intervalLengthUnit = null;
						for (String dataNode : dataNodes) {

							dataNodeCount++;

							logger.trace("Found data node:  clusterId={}; serviceId={}; nodeId={}", clusterId,
							        serviceId, dataNode);

							String dataPath = null;
							MetricsData metricsData = null;

							dataPath = pathScheme.getAbsolutePath(PathType.METRICS,
							        pathScheme.joinTokens(clusterId, serviceId, dataNode));

							try {
								metricsData = getMetricsFromDataNode(clusterId, serviceId, dataNode);
								if (metricsData == null) {
									continue;
								}
							} catch (Exception e) {
								logger.warn("Error trying to aggregate data directory for service:  clusterId="
								        + clusterId + "; serviceId=" + serviceId + ":  " + e, e);
								continue;
							}

							// skip data node if not within interval
							long millisToExpiry = millisToExpiry(metricsData, currentTimestamp);
							if (millisToExpiry <= 0) {
								continue;

							}

							intervalLength = metricsData.getIntervalLength();
							intervalLengthUnit = metricsData.getIntervalLengthUnit();

							// aggregate service stats for data nodes that
							// within current rotation interval
							logger.trace("Aggregating data node:  path={}; millisToExpiry={}", dataPath, millisToExpiry);

							// increment node count
							dataNodeInWindowCount++;

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
							// if (counterList.size() != dataNodeCount) {
							// logger.warn(
							// "counterList size does not match nodeCount:  counterList.size={}; nodeCount={}",
							// counterList.size(), dataNodeCount);
							// }
							CounterData counterData = CounterData.merge(counterList);
							counters.put(key, counterData);
						}
						serviceMetricsData.setCounters(counters);

						// gauges
						Map<String, GaugeData> gauges = new HashMap<String, GaugeData>(gaugeMap.size() + 1, 1.0f);
						for (String key : gaugeMap.keySet()) {
							List<GaugeData> gaugeList = gaugeMap.get(key);
							// if (gaugeList.size() != dataNodeCount) {
							// logger.warn(
							// "gaugeList size does not match nodeCount:  gaugeList.size={}; nodeCount={}",
							// gaugeList.size(), dataNodeCount);
							// }
							GaugeData gaugeData = GaugeData.merge(gaugeList);
							gauges.put(key, gaugeData);
						}
						serviceMetricsData.setGauges(gauges);

						// histograms
						Map<String, HistogramData> histograms = new HashMap<String, HistogramData>(
						        histogramMap.size() + 1, 1.0f);
						for (String key : histogramMap.keySet()) {
							List<HistogramData> histogramList = histogramMap.get(key);
							// if (histogramList.size() != dataNodeCount) {
							// logger.warn(
							// "histogramList size does not match nodeCount:  histogramList.size={}; nodeCount={}",
							// histogramList.size(), dataNodeCount);
							// }
							HistogramData histogramData = HistogramData.merge(histogramList);
							histograms.put(key, histogramData);
						}
						serviceMetricsData.setHistograms(histograms);

						// meters
						Map<String, MeterData> meters = new HashMap<String, MeterData>(meterMap.size() + 1, 1.0f);
						for (String key : meterMap.keySet()) {
							List<MeterData> meterList = meterMap.get(key);
							// if (meterList.size() != dataNodeCount) {
							// logger.warn(
							// "meterList size does not match nodeCount:  meterList.size={}; nodeCount={}",
							// meterList.size(), dataNodeCount);
							// }
							MeterData meterData = MeterData.merge(meterList);
							meters.put(key, meterData);
						}
						serviceMetricsData.setMeters(meters);

						// timers
						Map<String, TimerData> timers = new HashMap<String, TimerData>(timerMap.size() + 1, 1.0f);
						for (String key : timerMap.keySet()) {
							List<TimerData> timerList = timerMap.get(key);
							// if (timerList.size() != dataNodeCount) {
							// logger.warn(
							// "timerList size does not match nodeCount:  timerList.size={}; nodeCount={}",
							// timerList.size(), dataNodeCount);
							// }
							TimerData timerData = TimerData.merge(timerList);
							timers.put(key, timerData);
						}
						serviceMetricsData.setTimers(timers);

						serviceMetricsData.setDataNodeCount(dataNodeCount);
						serviceMetricsData.setDataNodeInWindowCount(dataNodeInWindowCount);
						serviceMetricsData.setClusterId(clusterId);
						serviceMetricsData.setServiceId(serviceId);
						serviceMetricsData.setIntervalLength(intervalLength);
						serviceMetricsData.setIntervalLengthUnit(intervalLengthUnit);
						serviceMetricsData.setLastUpdatedTimestamp(System.currentTimeMillis());

						// write to ZK
						String dataPath = pathScheme.getAbsolutePath(PathType.METRICS,
						        pathScheme.joinTokens(clusterId, serviceId));
						String serviceMetricsDataString = JacksonUtil.getObjectMapper().writeValueAsString(
						        serviceMetricsData);
						zkClientUtil.updatePath(getContext().getZkClient(), getContext().getPathScheme(), dataPath,
						        serviceMetricsDataString.getBytes(UTF_8), getContext().getDefaultZkAclList(),
						        CreateMode.PERSISTENT, -1);

						// sleep to hold lock before next interval so that
						// updates don't happen too frequently with
						// more nodes in service
						if (i == memberServiceIds.size() - 1) {
							try {
								long elapsedMillis = (System.nanoTime() - startTimeNanos) / 1000000;
								long sleepIntervalMillis = (updateIntervalMillis - elapsedMillis) / 2;
								if (sleepIntervalMillis < 0) {
									sleepIntervalMillis = updateIntervalMillis;
								}
								logger.debug(
								        "AggregationRunnable SLEEPING btw. services:  sleepIntervalMillis={}; memberServiceIds.size={}",
								        sleepIntervalMillis, memberServiceIds.size());
								Thread.sleep(sleepIntervalMillis);

							} catch (InterruptedException e) {
								logger.warn("Interrupted while sleeping at end of aggregation:  " + e, e);
							}
						}

					} catch (KeeperException e) {
						if (e.code() != KeeperException.Code.NONODE) {
							logger.warn("Error trying to aggregate data directory for service:  clusterId=" + clusterId
							        + "; serviceId=" + serviceId + ":  " + e, e);
						}
					} catch (Exception e) {
						logger.warn("Error trying to aggregate data directory for service:  clusterId=" + clusterId
						        + "; serviceId=" + serviceId + ":  " + e, e);
					} finally {
						logger.trace("Releasing lock:  metrics-aggregation-{}-{}", clusterId, serviceId);
						lock.unlock();
						lock.destroy();
						logger.trace("Released and destroyed lock:  metrics-aggregation-{}-{}", clusterId, serviceId);
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
				if (!presenceService.isMemberOf(clusterId)
				        || clusterId.equals(getContext().getPathScheme().getFrameworkClusterId())) {
					continue;
				}

				List<String> serviceIds = presenceService.getServices(clusterId);
				for (String serviceId : serviceIds) {
					logger.trace("Checking data nodes expiry:  clusterId={}; serviceId={}", clusterId, serviceId);

					// only proceed if in service
					if (!presenceService.isMemberOf(clusterId, serviceId)) {
						continue;
					}

					long currentTimestamp = System.currentTimeMillis();

					// get lock for a service
					DistributedLock lock = coordinationService.getLock("reign", "metrics-" + clusterId + "-"
					        + serviceId);
					if (!lock.tryLock()) {
						continue;
					}
					String dataPath = null;
					try {

						// get all data nodes for a service
						String dataParentPath = pathScheme.getAbsolutePath(PathType.METRICS,
						        pathScheme.joinTokens(clusterId, serviceId));
						List<String> dataNodes = zkClient.getChildren(dataParentPath, false);

						// remove all nodes that are older than rotation
						// interval
						for (String dataNode : dataNodes) {
							try {
								logger.trace("Checking data node expiry:  clusterId={}; serviceId={}; nodeId={}",
								        clusterId, serviceId, dataNode);
								dataPath = pathScheme.getAbsolutePath(PathType.METRICS,
								        pathScheme.joinTokens(clusterId, serviceId, dataNode));
								MetricsData metricsData = getMetricsFromDataNode(clusterId, serviceId, dataNode);
								if (metricsData == null) {
									logger.warn("Removing unrecognized/corrupted/deprecated data node:  path={}",
									        dataPath);
									zkClient.delete(dataPath, -1);
									continue;
								}

								// keep last few hours worth of data
								long millisToExpiry = millisToExpiry(metricsData, currentTimestamp - (86400000 / 6));

								// delete data that is older than some threshold
								boolean dataTooOld = currentTimestamp - metricsData.getIntervalStartTimestamp() > 86400000;

								// delete old and expired data
								if (millisToExpiry <= 0 || dataTooOld) {
									logger.info("Removing expired data node:  path={}; millisToExpiry={}", dataPath,
									        millisToExpiry);
									zkClient.delete(dataPath, -1);
								} else {
									logger.trace("Data node is not yet expired:  path={}; millisToExpiry={}", dataPath,
									        millisToExpiry);
								}
							} catch (Exception e) {
								logger.warn(
								        "Error trying to clean up data directory for service:  clusterId=" + clusterId
								                + "; serviceId=" + serviceId + "; dataPath=" + dataPath + ":  " + e, e);
							} // try
						}// for

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
