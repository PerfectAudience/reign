package io.reign.data;

import io.reign.AbstractActiveService;
import io.reign.DataSerializer;
import io.reign.JsonDataSerializer;
import io.reign.coord.CoordinationService;
import io.reign.coord.DistributedReadWriteLock;
import io.reign.mesg.RequestMessage;
import io.reign.mesg.ResponseMessage;

import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class DataService extends AbstractActiveService {

    private static final Logger logger = LoggerFactory.getLogger(DataService.class);

    /** queue of cache and persistent store operations to perform */
    private final LinkedBlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<Runnable>(512);

    /** process task queue every 2 seconds by default */
    public static final int DEFAULT_EXECUTION_INTERVAL_MILLIS = 2000;

    private static final String DATA_PATH_SUFFIX = "$";
    private static final String MAP_PATH_SUFFIX = "{}";
    private static final String LIST_PATH_SUFFIX = "[]";

    private DataSerializer<DataValue> dataSerializer = new JsonDataSerializer<DataValue>();

    public <V> MultiData<V> get(String clusterId, String dataPath, int ttlMillis, boolean processSafe) {
        dataPath = dataPath + DATA_PATH_SUFFIX;

        DistributedReadWriteLock readWriteLock = null;
        if (processSafe) {
            readWriteLock = getReadWriteLock(clusterId, dataPath);
        }

        String relativeBasePath = getPathScheme().joinPaths(clusterId, dataPath);

        return new ZkMultiData<V>(relativeBasePath, getPathScheme(), readWriteLock, getZkClient(), ttlMillis);
    }

    public <K, V> MultiMapData<K, V> getMultiMap(String clusterId, String dataPath, boolean processSafe) {
        dataPath = dataPath + MAP_PATH_SUFFIX;

        DistributedReadWriteLock readWriteLock = null;
        if (processSafe) {
            readWriteLock = getReadWriteLock(clusterId, dataPath);
        }

        String relativeBasePath = getPathScheme().joinPaths(clusterId, dataPath);

        return new ZkMultiMapData<K, V>(relativeBasePath, getPathScheme(), readWriteLock, getZkClient());
    }

    public <V> LinkedListData<V> getLinkedList(String clusterId, String dataPath, int maxSize, int ttlMillis,
            boolean processSafe) {
        if (maxSize == -1) {
            dataPath = dataPath + LIST_PATH_SUFFIX;
        } else {
            dataPath = dataPath + LIST_PATH_SUFFIX.charAt(0) + maxSize + LIST_PATH_SUFFIX.charAt(1);
        }

        DistributedReadWriteLock readWriteLock = null;
        if (processSafe) {
            readWriteLock = getReadWriteLock(clusterId, dataPath);
        }

        String relativeBasePath = getPathScheme().joinPaths(clusterId, dataPath);

        return new ZkLinkedListData<V>(maxSize, relativeBasePath, getPathScheme(), readWriteLock, getZkClient(),
                ttlMillis);
    }

    public <V> QueueData<V> getQueue(String clusterId, String dataPath, int maxSize, int ttlMillis, boolean processSafe) {
        LinkedListData<V> linkedListData = getLinkedList(clusterId, dataPath, maxSize, ttlMillis, processSafe);

        return new ZkQueueData<V>(linkedListData);
    }

    public <V> StackData<V> getStack(String clusterId, String dataPath, int maxSize, int ttlMillis, boolean processSafe) {
        LinkedListData<V> linkedListData = getLinkedList(clusterId, dataPath, maxSize, ttlMillis, processSafe);

        return new ZkStackData<V>(linkedListData);
    }

    DistributedReadWriteLock getReadWriteLock(String clusterId, String dataPath) {
        CoordinationService coordinationService = getContext().getService("coord");
        DistributedReadWriteLock readWriteLock = coordinationService.getReadWriteLock(clusterId, dataPath);
        return readWriteLock;
    }

    @Override
    public void perform() {
        /** check data cache and see if we need to do reads to keep data cache warm **/

        /** do operations in task queue **/

        /** groom data **/
        // get lock on path, check data point TTLs, and clean up data nodes as necessary
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

    public DataSerializer<DataValue> getDataSerializer() {
        return dataSerializer;
    }

    public void setDataSerializer(DataSerializer<DataValue> dataSerializer) {
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

}
