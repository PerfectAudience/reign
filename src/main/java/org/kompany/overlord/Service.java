package org.kompany.overlord;

import org.kompany.overlord.util.PathCache;

/**
 * Common interface for framework service plug-ins.<br/>
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
public interface Service {

    public void setPathCache(PathCache pathCache);

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
