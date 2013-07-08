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

package io.reign.data;

import io.reign.AbstractActiveService;
import io.reign.DataSerializer;
import io.reign.PathScheme;
import io.reign.PathType;
import io.reign.coord.CoordinationService;
import io.reign.coord.DistributedReadWriteLock;
import io.reign.mesg.RequestMessage;
import io.reign.mesg.ResponseMessage;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.zookeeper.data.ACL;
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

    private final Map<String, DataSerializer> dataSerializerMap = new ConcurrentHashMap<String, DataSerializer>(33,
            0.9f, 1);

    public DataService() {
        // register default serializers
        dataSerializerMap.put(Long.class.getName(), new LongSerializer());
        dataSerializerMap.put(Integer.class.getName(), new IntegerSerializer());
        dataSerializerMap.put(Float.class.getName(), new FloatSerializer());
        dataSerializerMap.put(Double.class.getName(), new DoubleSerializer());
        dataSerializerMap.put(Boolean.class.getName(), new BooleanSerializer());
        dataSerializerMap.put(Short.class.getName(), new ShortSerializer());
        dataSerializerMap.put(Byte.class.getName(), new ByteSerializer());
        dataSerializerMap.put(byte[].class.getName(), new BytesSerializer());
        dataSerializerMap.put(String.class.getName(), new Utf8StringSerializer());

    }

    public <V> MultiData<V> getMulti(String clusterId, String dataPath, Class<V> typeClass, boolean processSafe) {
        return getMulti(clusterId, dataPath, typeClass, processSafe, getContext().getDefaultZkAclList());
    }

    public <V> MultiData<V> getMulti(String clusterId, String dataPath, Class<V> typeClass, boolean processSafe,
            List<ACL> aclList) {
        dataPath = dataPath + DATA_PATH_SUFFIX;

        DistributedReadWriteLock readWriteLock = null;
        if (processSafe) {
            readWriteLock = getReadWriteLock(clusterId, dataPath);
        }

        PathScheme pathScheme = getPathScheme();
        String absoluteBasePath = pathScheme.getAbsolutePath(PathType.DATA, pathScheme.joinTokens(clusterId, dataPath));

        return new ZkMultiData<V>(absoluteBasePath, readWriteLock, aclList, dataSerializerMap, getContext());
    }

    public <K> MultiMapData<K> getMultiMap(String clusterId, String dataPath, boolean processSafe, List<ACL> aclList) {
        dataPath = dataPath + MAP_PATH_SUFFIX;

        DistributedReadWriteLock readWriteLock = null;
        if (processSafe) {
            readWriteLock = getReadWriteLock(clusterId, dataPath);
        }

        PathScheme pathScheme = getPathScheme();
        String absoluteBasePath = pathScheme.getAbsolutePath(PathType.DATA, pathScheme.joinTokens(clusterId, dataPath));

        return new ZkMultiMapData<K>(absoluteBasePath, readWriteLock, aclList, dataSerializerMap, getContext());
    }

    public <V> LinkedListData<V> getLinkedList(String clusterId, String dataPath, int maxSize, int ttlMillis,
            boolean processSafe, List<ACL> aclList) {
        if (maxSize == -1) {
            dataPath = dataPath + LIST_PATH_SUFFIX;
        } else {
            dataPath = dataPath + LIST_PATH_SUFFIX.charAt(0) + maxSize + LIST_PATH_SUFFIX.charAt(1);
        }

        DistributedReadWriteLock readWriteLock = null;
        if (processSafe) {
            readWriteLock = getReadWriteLock(clusterId, dataPath);
        }

        PathScheme pathScheme = getPathScheme();
        String absoluteBasePath = pathScheme.getAbsolutePath(PathType.DATA, pathScheme.joinTokens(clusterId, dataPath));

        return new ZkLinkedListData<V>(maxSize, absoluteBasePath, getPathScheme(), readWriteLock, getZkClient(),
                getPathCache(), ttlMillis);
    }

    public <V> QueueData<V> getQueue(String clusterId, String dataPath, int maxSize, int ttlMillis,
            boolean processSafe, List<ACL> aclList) {
        LinkedListData<V> linkedListData = getLinkedList(clusterId, dataPath, maxSize, ttlMillis, processSafe, aclList);

        return new ZkQueueData<V>(linkedListData);
    }

    public <V> StackData<V> getStack(String clusterId, String dataPath, int maxSize, int ttlMillis,
            boolean processSafe, List<ACL> aclList) {
        LinkedListData<V> linkedListData = getLinkedList(clusterId, dataPath, maxSize, ttlMillis, processSafe, aclList);

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

    public void registerSerializer(String className, DataSerializer dataSerializer) {
        this.dataSerializerMap.put(className, dataSerializer);
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
