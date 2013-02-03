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
public interface ActiveService extends Service {

    /**
     * 
     * @return 0 if this plug-in is meant to run continuously (framework will
     *         allocate a thread for it); otherwise, the plug-in will share a
     *         thread pool with other plug-ins.
     */
    public long getExecutionIntervalMillis();

    /**
     * Run periodically.
     */
    public void perform();
}
