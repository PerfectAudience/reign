package org.kompany.overlord;

/**
 * 
 * @author ypai
 * 
 */
public abstract class AbstractService implements Service {

    private PathScheme pathScheme;
    private ZkClient zkClient;

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
