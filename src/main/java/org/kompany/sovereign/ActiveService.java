package org.kompany.sovereign;

/**
 * A service that runs continuously or performs tasks periodically at regular
 * intervals.
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
