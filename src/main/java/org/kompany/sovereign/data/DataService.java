package org.kompany.sovereign.data;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.kompany.sovereign.AbstractActiveService;
import org.kompany.sovereign.CanonicalNodeId;
import org.kompany.sovereign.DataSerializer;
import org.kompany.sovereign.JsonDataSerializer;
import org.kompany.sovereign.PathContext;
import org.kompany.sovereign.PathType;
import org.kompany.sovereign.coord.CoordinationService;
import org.kompany.sovereign.coord.DistributedLock;
import org.kompany.sovereign.presence.PresenceService;
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
    private final Map<CanonicalNodeId, DataCollector> collectorMap = new ConcurrentHashMap<CanonicalNodeId, DataCollector>(
            4, 0.9f, 1);

    /** set of clusterId(s) for which to perform data aggregation */
    private final Set<String> activeAggregationClusterIds = Collections
            .newSetFromMap(new ConcurrentHashMap<String, Boolean>(4, 0.9f, 1));

    private DataSerializer<Datum> dataSerializer = new JsonDataSerializer<Datum>();

    public Datum getDatum(String clusterId, String serviceId) {
        return null;
    }

    public Datum getDatum(String clusterId, String serviceId, String nodeId) {
        return null;
    }

    public void register(String clusterId, String serviceId, String nodeId, DataCollector collector) {
        // add cluster id to set of cluster ids to do aggregation for
        activeAggregationClusterIds.add(clusterId);

        collectorMap.put(new CanonicalNodeId(clusterId, serviceId, nodeId), collector);
    }

    @Override
    public void perform() {
        /** iterate through collectors, serialize, and set in ZK **/
        Iterator<CanonicalNodeId> keyIter = collectorMap.keySet().iterator();
        while (keyIter.hasNext()) {
            try {
                CanonicalNodeId key = keyIter.next();
                DataCollector collector = collectorMap.get(key);

                byte[] data = dataSerializer.serialize(collector.get());

                getZkClient().setData(key.toPathString(getPathScheme()), data, -1);

            } catch (Exception e) {
                logger.error("Error while serializing data:  " + e, e);
            }
        }// while

        /** acquire aggregation lock and aggregate data to service level **/
        PresenceService presenceService = getServiceDirectory().getService("presence");
        CoordinationService coordinationService = getServiceDirectory().getService("coord");
        for (String clusterId : activeAggregationClusterIds) {
            List<String> services = presenceService.getAvailableServices(clusterId);
            for (String serviceId : services) {
                // get lock to aggregate service node data
                DistributedLock aggregateLock = coordinationService.getLock(PathContext.INTERNAL, getSovereignId(),
                        "data", "aggregate-" + clusterId + "-" + serviceId, getDefaultAclList());
                aggregateLock.lock();
                try {
                    // get service child nodes
                    String relativeServicePath = getPathScheme().buildRelativePath(clusterId, serviceId);
                    String servicePath = getPathScheme().getAbsolutePath(PathContext.USER, PathType.PRESENCE,
                            relativeServicePath);
                    List<String> children = getZkClient().getChildren(servicePath, false);

                    // iterate through children
                    for (String nodeId : children) {
                        // read datum from child
                        String relativeServiceNodePath = getPathScheme()
                                .buildRelativePath(clusterId, serviceId, nodeId);
                        String serviceNodePath = getPathScheme().getAbsolutePath(PathContext.USER, PathType.PRESENCE,
                                relativeServiceNodePath);
                        byte[] bytes = getZkClient().getData(serviceNodePath, false, null);

                        // deserialize
                        Datum datum = dataSerializer.deserialize(bytes);

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
        if (this.getExecutionIntervalMillis() < 1000) {
            this.setExecutionIntervalMillis(DEFAULT_EXECUTION_INTERVAL_MILLIS);
        }
    }

    @Override
    public void destroy() {

    }

    public DataSerializer<Datum> getDataSerializer() {
        return dataSerializer;
    }

    public void setDataSerializer(DataSerializer<Datum> dataSerializer) {
        this.dataSerializer = dataSerializer;
    }

}
