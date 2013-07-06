package io.reign.data;

import io.reign.AbstractActiveService;
import io.reign.DataSerializer;
import io.reign.JsonDataSerializer;
import io.reign.mesg.RequestMessage;
import io.reign.mesg.ResponseMessage;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

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

    private final DataSerializer<PointData> dataSerializer = new JsonDataSerializer<PointData>();

    private final ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(2);

    /** data cache */
    private final ConcurrentLinkedHashMap<String, DataValue<?>> dataCache = new ConcurrentLinkedHashMap.Builder<String, DataValue<?>>()
            .maximumWeightedCapacity(4096).initialCapacity(1024).concurrencyLevel(2).build();

    /**
     * Retrieve a single data path. Example usage:
     * <ul>
     * <li>Returns DataNode (could be Map or List of DataPoints or just a single value):
     * dataService.getDataPack("myCluster", "eventServer", "someHostNameId");</li>
     * <li>Returns DataNode (could be Map or List of DataPoints or just a single value):
     * dataService.getDataPack("myCluster", "eventServer/eventCounts", "someHostNameId");</li>
     * <li>Returns DataPoint: dataService.getDataPack("myCluster", "eventServer/eventCounts", "someHostNameId.mapKey");</li>
     * <li>Returns DataPoint: dataService.getDataPack("myCluster", "eventServer/eventCounts",
     * "someHostNameId.listIndex");</li>
     * </ul>
     * 
     * @param clusterId
     * @param dataKeyPath
     * @return read-only data
     */
    public <T> T getData(String clusterId, String dataNodePath) {
        // a "." in the

        return null;
    }

    /**
     * Apply an operation to a set of DataPoint(s).
     * 
     * @param <T>
     * @param operation
     * @param clusterId
     * @param dataKeyPath
     * @return
     */
    public <T> T group(Operator operation, String clusterId, String dataNodePath) {
        return null;
    }

    @Override
    public void perform() {
        /** check data cache and see if we need to do reads to keep data cache warm **/

        /** do operations in task queue **/
        Runnable runnable = null;
        do {
            if (runnable != null) {
                this.executorService.submit(runnable);
            }
        } while (runnable != null);

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
        executorService.shutdown();
    }

    // public DataSerializer<DataBundle> getDataSerializer() {
    // return dataSerializer;
    // }
    //
    // public void setDataSerializer(DataSerializer<DataBundle> dataSerializer) {
    // this.dataSerializer = dataSerializer;
    // }

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

    private static abstract class DataServiceRunnable implements Runnable {
        private static final AtomicInteger outstandingOperations = new AtomicInteger(0);

        private int delayMillis;

        private String path;

        public abstract void doRun();

        public int getDelayMillis() {
            return delayMillis;
        }

        public void setDelayMillis(int delayMillis) {
            this.delayMillis = delayMillis;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public static AtomicInteger getOutstandingoperations() {
            return outstandingOperations;
        }

        @Override
        public void run() {
            outstandingOperations.incrementAndGet();
            doRun();
            outstandingOperations.decrementAndGet();
        }
    }

}
