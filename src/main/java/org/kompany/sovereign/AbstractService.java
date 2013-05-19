package org.kompany.sovereign;

import java.util.List;

import org.apache.zookeeper.data.ACL;
import org.kompany.sovereign.util.PathCache;
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

    private ServiceDirectory serviceDirectory;

    private List<ACL> defaultAclList;

    private String canonicalId;

    public List<ACL> getDefaultAclList() {
        return defaultAclList;
    }

    @Override
    public void setDefaultAclList(List<ACL> defaultAclList) {
        this.defaultAclList = defaultAclList;
    }

    @Override
    public void setCanonicalId(String canonicalId) {
        this.canonicalId = canonicalId;
    }

    public String getCanonicalId() {
        return canonicalId;
    }

    public ServiceDirectory getServiceDirectory() {
        return serviceDirectory;
    }

    @Override
    public void setServiceDirectory(ServiceDirectory serviceDirectory) {
        this.serviceDirectory = serviceDirectory;
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
