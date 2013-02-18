package org.kompany.overlord.conf;

import java.util.List;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.kompany.overlord.AbstractService;
import org.kompany.overlord.PathType;
import org.kompany.overlord.Sovereign;
import org.kompany.overlord.util.PathCache.PathCacheEntry;
import org.kompany.overlord.util.ZkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfService extends AbstractService implements Watcher {

    private static final Logger logger = LoggerFactory.getLogger(ConfService.class);

    private ZkUtil zkUtil = new ZkUtil();

    /**
     * 
     * @param relativePath
     * @param confSerializer
     * @return
     */
    public <T> T getConf(String relativePath, ConfSerializer<T> confSerializer) {
        return getConfAbsolutePath(getPathScheme().getAbsolutePath(PathType.CONF, relativePath), confSerializer);

    }

    /**
     * 
     * @param relativePath
     * @param conf
     * @param confSerializer
     */
    public <T> void putConf(String relativePath, T conf, ConfSerializer<T> confSerializer) {
        putConfAbsolutePath(getPathScheme().getAbsolutePath(PathType.CONF, relativePath), conf, confSerializer,
                Sovereign.DEFAULT_ACL_LIST);
    }

    /**
     * 
     * @param relativePath
     * @param conf
     * @param confSerializer
     * @param aclList
     */
    public <T> void putConf(String relativePath, T conf, ConfSerializer<T> confSerializer, List<ACL> aclList) {
        putConfAbsolutePath(getPathScheme().getAbsolutePath(PathType.CONF, relativePath), conf, confSerializer, aclList);
    }

    /**
     * 
     * @param absolutePath
     * @param confSerializer
     * @return
     */
    <T> T getConfAbsolutePath(String absolutePath, ConfSerializer<T> confSerializer) {
        byte[] bytes = null;
        try {
            PathCacheEntry pathCacheEntry = getPathCache().get(absolutePath);
            if (pathCacheEntry != null) {
                // found in cache
                bytes = pathCacheEntry.getBytes();
            } else {
                // not in cache, so load from ZK
                Stat stat = new Stat();
                bytes = getZkClient().getData(absolutePath, true, stat);

                // put in cache
                getPathCache().put(absolutePath, stat, bytes, null);
            }

            return bytes != null ? confSerializer.deserialize(bytes) : null;

        } catch (KeeperException e) {
            if (e.code() == Code.NONODE) {
                logger.debug(
                        "getConfAbsolutePath():  error trying to fetch node info:  {}:  node does not exist:  path={}",
                        e.getMessage(), absolutePath);
            } else {
                logger.error("getConfAbsolutePath():  error trying to fetch node info:  " + e, e);
            }
            return null;
        } catch (Exception e) {
            logger.error("getConfAbsolutePath():  error trying to fetch node info:  " + e, e);
            return null;
        }

    }

    /**
     * 
     * @param absolutePath
     * @param conf
     * @param confSerializer
     * @param aclList
     */
    <T> void putConfAbsolutePath(String absolutePath, T conf, ConfSerializer<T> confSerializer, List<ACL> aclList) {
        try {
            // write to ZK
            byte[] leafData = confSerializer.serialize(conf);
            String pathUpdated = zkUtil.updatePath(getZkClient(), getPathScheme(), absolutePath, leafData, aclList,
                    CreateMode.PERSISTENT, -1);

            logger.debug("putConfAbsolutePath():  saved configuration:  path={}", pathUpdated);
        } catch (Exception e) {
            logger.error("putConfAbsolutePath():  error while saving configuration:  " + e + ":  path=" + absolutePath,
                    e);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        // log if DEBUG
        if (logger.isDebugEnabled()) {
            logger.debug("***** Received ZooKeeper Event:\n"
                    + ReflectionToStringBuilder.toString(event, ToStringStyle.DEFAULT_STYLE));

        }
    }

    @Override
    public void init() {
        // TODO Auto-generated method stub

    }

    @Override
    public void destroy() {
        // TODO Auto-generated method stub

    }

}