package io.reign;

import io.reign.messaging.RequestMessage;
import io.reign.messaging.ResponseMessage;
import io.reign.util.PathCache;

import java.util.List;

import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public abstract class AbstractService extends AbstractZkEventHandler implements Service {

    private static final Logger logger = LoggerFactory.getLogger(AbstractService.class);

    private PathScheme pathScheme;
    private ZkClient zkClient;

    private PathCache pathCache;

    private ReignContext context;

    private List<ACL> defaultAclList;

    @Override
    public ResponseMessage handleMessage(RequestMessage message) {
        return null;
    }

    public List<ACL> getDefaultAclList() {
        return defaultAclList;
    }

    @Override
    public void setDefaultAclList(List<ACL> defaultAclList) {
        this.defaultAclList = defaultAclList;
    }

    @Override
    public ReignContext getContext() {
        return context;
    }

    @Override
    public void setContext(ReignContext serviceDirectory) {
        this.context = serviceDirectory;
    }

    public PathCache getPathCache() {
        return pathCache;
    }

    @Override
    public void setPathCache(PathCache pathCache) {
        this.pathCache = pathCache;
    }

    public PathScheme getPathScheme() {
        return pathScheme;
    }

    @Override
    public void setPathScheme(PathScheme pathScheme) {
        this.pathScheme = pathScheme;

    }

    public ZkClient getZkClient() {
        return zkClient;
    }

    @Override
    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    @Override
    public abstract void init();

    @Override
    public abstract void destroy();

}
