package io.reign.data;

import io.reign.AbstractActiveService;
import io.reign.DataSerializer;
import io.reign.JsonDataSerializer;
import io.reign.PathType;
import io.reign.coord.CoordinationService;
import io.reign.coord.DistributedLock;
import io.reign.messaging.RequestMessage;
import io.reign.messaging.ResponseMessage;
import io.reign.presence.PresenceService;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class DataService extends AbstractActiveService {

    private static final Logger logger = LoggerFactory.getLogger(DataService.class);

    /** publish every 5 seconds by default */
    public static final int DEFAULT_EXECUTION_INTERVAL_MILLIS = 5000;

    /** map of data collectors that gather node data to publish to ZK */
    private final Map<String, DataCollector> collectorMap = new ConcurrentHashMap<String, DataCollector>(4, 0.9f, 1);

    /** set of clusterId(s) for which to perform data aggregation */
    private final Set<String> activeAggregationClusterIds = Collections
            .newSetFromMap(new ConcurrentHashMap<String, Boolean>(4, 0.9f, 1));

    private DataSerializer<DataBundle> dataSerializer = new JsonDataSerializer<DataBundle>();

    /**
     * Retrieves DataBundle for a given service.
     * 
     * @param clusterId
     * @param serviceId
     * @return
     */
    public DataBundle getDataBundle(String clusterId, String serviceId) {
        return null;
    }

    /**
     * Retrieves DataBundle for a given service node.
     * 
     * @param clusterId
     * @param serviceId
     * @param nodeId
     * @return
     */
    public DataBundle getDataBundle(String clusterId, String serviceId, String nodeId) {
        return null;
    }

    public void register(String clusterId, String serviceId, String nodeId, DataCollector collector) {
        // add cluster id to set of cluster ids to do aggregation for
        activeAggregationClusterIds.add(clusterId);

        collectorMap.put(getNodeIdString(clusterId, serviceId, nodeId), collector);
    }

    @Override
    public void perform() {
        /** iterate through collectors, serialize, and set in ZK **/
        Iterator<String> keyIter = collectorMap.keySet().iterator();
        while (keyIter.hasNext()) {
            try {
                String key = keyIter.next();
                DataCollector collector = collectorMap.get(key);

                byte[] data = dataSerializer.serialize(collector.get());

                // gather data

            } catch (Exception e) {
                logger.error("Error while serializing data:  " + e, e);
            }
        }// while

        /** acquire aggregation lock and aggregate data to service level **/
        PresenceService presenceService = getContext().getService("presence");
        CoordinationService coordinationService = getContext().getService("coord");
        for (String clusterId : activeAggregationClusterIds) {

            // skip reserved cluster id
            if (clusterId.equals(getContext().getReservedClusterId())) {
                continue;
            }

            // iterate through available services
            List<String> services = presenceService.lookupServices(clusterId);
            for (String serviceId : services) {
                // get lock to aggregate service node data
                DistributedLock aggregateLock = coordinationService.getLock(getPathScheme().getCanonicalId(), "data",
                        "aggregate-" + clusterId + "-" + serviceId, getDefaultAclList());
                aggregateLock.lock();
                try {
                    // get service child nodes
                    String relativeServicePath = getPathScheme().buildRelativePath(clusterId, serviceId);
                    String servicePath = getPathScheme().getAbsolutePath(PathType.PRESENCE, relativeServicePath);
                    List<String> children = getZkClient().getChildren(servicePath, false);

                    // iterate through children
                    for (String nodeId : children) {
                        // read datum from child
                        String relativeServiceNodePath = getPathScheme()
                                .buildRelativePath(clusterId, serviceId, nodeId);
                        String serviceNodePath = getPathScheme().getAbsolutePath(PathType.PRESENCE,
                                relativeServiceNodePath);
                        byte[] bytes = getZkClient().getData(serviceNodePath, false, null);

                        // deserialize
                        DataBundle dataBundle = dataSerializer.deserialize(bytes);

                        // aggregate
                    }

                    // write out service level datum

                } catch (Exception e) {
                } finally {
                    aggregateLock.unlock();
                    aggregateLock.destroy();
                }
            } // for
        }// for
    }

    @Override
    public void init() {
        // minimum of 1 second intervals between updating data
        if (this.getExecutionIntervalMillis() < 1000) {
            this.setExecutionIntervalMillis(DEFAULT_EXECUTION_INTERVAL_MILLIS);
        }

    }

    @Override
    public void destroy() {

    }

    public DataSerializer<DataBundle> getDataSerializer() {
        return dataSerializer;
    }

    public void setDataSerializer(DataSerializer<DataBundle> dataSerializer) {
        this.dataSerializer = dataSerializer;
    }

    @Override
    public ResponseMessage handleMessage(RequestMessage requestMessage) {
        if (logger.isTraceEnabled()) {
            try {
                logger.trace("Received message:  request='{}:{}'", requestMessage.getTargetService(), requestMessage
                        .getBody());
            } catch (Exception e) {
                logger.error("" + e, e);
            }
        }

        return null;
    }

    String getNodeIdString(String clusterId, String serviceId, String nodeId) {
        return getPathScheme().buildRelativePath(clusterId, serviceId, nodeId);
    }

}
