package io.reign.data;

import io.reign.DataSerializer;
import io.reign.PathScheme;
import io.reign.ZkClient;
import io.reign.util.PathCache;
import io.reign.util.ZkClientUtil;

import java.util.List;
import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
abstract class ZkLinkedListBase {

    private static final Logger logger = LoggerFactory.getLogger(ZkLinkedListBase.class);

    private final ZkClientUtil zkClientUtil = new ZkClientUtil();

    private Map<String, DataSerializer> dataSerializerMap;

    public String writeData(ZkClient zkClient, PathScheme pathScheme, String listPath, DataValue value,
            DataSerializer<DataValue> serializer, PathCache pathCache, List<ACL> aclList) {
        try {

            byte[] bytes = null;
            if (value.value() != null) {
                DataSerializer dataSerializer = dataSerializerMap.get(value.value().getClass());
                if (dataSerializer == null) {
                    throw new IllegalStateException("No data serializer/deserializer found for "
                            + value.value().getClass().getName());
                }
                bytes = dataSerializer.serialize(value.value());
            }

            // write data to ZK
            String absoluteDataPath = zkClientUtil.updatePath(zkClient, pathScheme, listPath, bytes, aclList,
                    CreateMode.PERSISTENT_SEQUENTIAL, -1);

            return absoluteDataPath;

        } catch (KeeperException e) {
            logger.error("" + e, e);
            return null;
        } catch (Exception e) {
            logger.error("" + e, e);
            return null;
        }

    }
}
