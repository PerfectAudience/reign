package org.kompany.sovereign;

import java.util.List;

import org.apache.zookeeper.data.ACL;
import org.kompany.sovereign.util.PathCache;

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

    public void setServiceDirectory(ServiceDirectory serviceDirectory);

    public void setPathCache(PathCache pathCache);

    public void setDefaultAclList(List<ACL> defaultAclList);

    public void setSovereignId(String sovereignId);

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
