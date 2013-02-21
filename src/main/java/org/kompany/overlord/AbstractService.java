package org.kompany.overlord;

import org.kompany.overlord.util.PathCache;

/**
 * 
 * @author ypai
 * 
 */
public abstract class AbstractService implements Service {

    private PathScheme pathScheme;
    private ZkClient zkClient;

    private PathCache pathCache;

    private ServiceDirectory serviceDirectory;

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
