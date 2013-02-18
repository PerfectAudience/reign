package org.kompany.overlord;

/**
 * A service that runs continuously or periodically on a regular interval.
 * 
 * @author ypai
 * 
 */
public interface ActiveService extends Service {

    /**
     * 
     * @return 0 if this plug-in is meant to run continuously (framework will allocate a thread for it); otherwise, the
     *         plug-in will share a thread pool with other plug-ins.
     */
    public long getExecutionIntervalMillis();

    /**
     * Run periodically.
     */
    public void perform();
}
