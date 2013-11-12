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

import io.reign.DataSerializer;
import io.reign.PathScheme;
import io.reign.ZkClient;
import io.reign.util.PathCache;
import io.reign.util.PathCacheEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ypai
 * 
 */
public class ZkClientLinkedListDataUtil extends ZkClientDataUtil {

    private static final Logger logger = LoggerFactory.getLogger(ZkClientLinkedListDataUtil.class);

    ZkClientLinkedListDataUtil(ZkClient zkClient, PathScheme pathScheme, PathCache pathCache,
            TranscodingScheme transcodingScheme) {
        super(zkClient, pathScheme, pathCache, transcodingScheme);

    }

    <V> V readData(String absoluteDataPath, int ttlMillis, Class<V> typeClass) {
        try {

            // try to get from path cache, use stat modified timestamp instead of cache entry modified timestamp because
            // we are more interested in when the data last changed
            byte[] bytes = null;
            if (bytes == null) {
                // read data from ZK
                Stat stat = new Stat();
                bytes = zkClient.getData(absoluteDataPath, true, stat);

                // see if item is expired
                if (isExpired(stat.getMtime(), ttlMillis)) {
                    return null;
                }

                // update in path cache
                pathCache.put(absoluteDataPath, stat, bytes, Collections.EMPTY_LIST);
            }

            // deserialize
            V data = null;
            if (bytes != null) {
                data = transcodingScheme.fromBytes(bytes, typeClass);
            }

            // logger.debug("readData():  absoluteDataPath={}; index={}; value={}", new Object[] { absoluteDataPath,
            // index, data });

            return data;

        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NONODE) {
                logger.error("" + e, e);
            }
            return null;
        } catch (Exception e) {
            logger.error("" + e, e);
            return null;
        }
    }

    <V> String writeData(String absoluteDataPath, V value, List<ACL> aclList) {
        try {

            byte[] bytes = null;
            if (value != null) {
                bytes = transcodingScheme.toBytes(value);
            } else {
                logger.warn("Attempting to write null data:  doing nothing:  absoluteDataPath={}; value={}",
                        new Object[] { absoluteDataPath, value });
                return null;
            }

            // write data to ZK
            AtomicReference<Stat> statRef = new AtomicReference<Stat>();
            String absoluteDataValuePath = updatePath(zkClient, pathScheme, absoluteDataPath + "/I_", bytes, aclList,
                    CreateMode.PERSISTENT_SEQUENTIAL, -1, statRef);

            // get stat if it was not returned from updatePath
            Stat stat = statRef.get();
            if (stat == null) {
                stat = zkClient.exists(absoluteDataValuePath, false);
            }

            // update stat with recent update time in case we have a stale read
            stat.setMtime(System.currentTimeMillis());

            // update path cache after successful write
            pathCache.put(absoluteDataValuePath, stat, bytes, null);

            PathCacheEntry pce = pathCache.get(absoluteDataValuePath);
            logger.debug("writeData():  absoluteDataValuePath={}; pathCacheEntry={}", absoluteDataValuePath,
                    transcodingScheme.fromBytes(pce.getBytes(), value.getClass()));

            return absoluteDataValuePath;

        } catch (KeeperException e) {
            logger.error("" + e, e);
            return null;
        } catch (Exception e) {
            logger.error("" + e, e);
            return null;
        }
    }

    String deleteData(String absoluteDataPath, int ttlMillis) {
        try {

            // try to get from path cache, use stat modified timestamp instead of cache entry modified timestamp because
            // we are more interested in when the data last changed
            Stat stat = null;
            if (ttlMillis > 0) {
                // read data from ZK
                stat = zkClient.exists(absoluteDataPath, true);

                // see if item is expired
                if (isExpired(stat.getMtime(), ttlMillis)) {
                    stat = null;
                }
            }

            // if bytes == null, meaning that data for this index is expired or that we are removing regardless of data
            // age, delete data node
            String deletedPath = null;
            if (stat == null) {
                zkClient.delete(absoluteDataPath, -1);
                deletedPath = absoluteDataPath;

                // remove node entry in path cache
                pathCache.remove(absoluteDataPath);

                // update parent children in path cache if parent node exists in cache
                String absoluteParentPath = pathScheme.getParentPath(absoluteDataPath);
                PathCacheEntry pathCacheEntry = pathCache.get(absoluteParentPath);
                if (pathCacheEntry != null) {
                    List<String> currentChildList = pathCacheEntry.getChildren();
                    List<String> newChildList = new ArrayList<String>(currentChildList.size());
                    for (String child : currentChildList) {
                        if (!deletedPath.endsWith(child)) {
                            newChildList.add(child);
                        }
                    }

                    pathCache
                            .put(absoluteParentPath, pathCacheEntry.getStat(), pathCacheEntry.getBytes(), newChildList);
                }
            }

            return deletedPath;

        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NONODE) {
                logger.error("" + e, e);
            }
            return null;
        } catch (Exception e) {
            logger.error("" + e, e);
            return null;
        }
    }

    List<String> getSortedChildList(String absoluteBasePath) {
        try {
            Stat stat = new Stat();
            List<String> childList = zkClient.getChildren(absoluteBasePath, true, stat);

            if (childList == null) {
                return Collections.EMPTY_LIST;
            }

            Collections.sort(childList);

            // update in path cache
            pathCache.put(absoluteBasePath, stat, null, childList);

            return childList;
        } catch (KeeperException e) {
            logger.error("" + e, e);
            return Collections.EMPTY_LIST;
        } catch (Exception e) {
            logger.error("" + e, e);
            return Collections.EMPTY_LIST;
        }
    }

}
