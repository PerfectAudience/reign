package org.kompany.overlord;


/**
 * Common interface for framework plug-ins.<br/>
 * <br/>
 * Life cycle for a service:<br/>
 * <ol>
 * <li> {@link setPathScheme()}
 * <li> {@link setZkClient()}
 * <li> {@link init()}
 * <li> {@link destroy()}
 * </ol>
 * 
 * @author ypai
 * 
 */
public interface Plugin extends Runnable {

    public static final long EXECUTION_INTERVAL_CONTINUOUS = 0;

    /**
     * 
     * @return 0 if this plug-in is meant to run continuously (framework will
     *         allocate a thread for it); otherwise, the plug-in will share a
     *         thread pool with other plug-ins.
     */
    public long getExecutionIntervalMillis();

    /**
     * 
     * @param pathScheme
     */
    public void setPathScheme(PathScheme pathScheme);

    /**
     * 
     * @param zkClient
     */
    public void setZkClient(ZkClient zkClient);

    /**
     * Initialize the service.
     */
    public void init();

    /**
     * Clean up resources as necessary.
     */
    public void destroy();
}
