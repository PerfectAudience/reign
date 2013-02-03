package org.kompany.overlord;

/**
 * 
 * @author ypai
 * 
 */
public abstract class AbstractPlugin implements Plugin {
    private long executionIntervalMillis = -1;
    private PathScheme pathScheme;
    private ZkClient zkClient;

    @Override
    public abstract void run();

    public void setExecutionIntervalMillis(long executionIntervalMillis) {
        this.executionIntervalMillis = executionIntervalMillis;
    }

    @Override
    public long getExecutionIntervalMillis() {
        return executionIntervalMillis;
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
