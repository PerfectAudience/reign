package io.reign;

import io.reign.mesg.RequestMessage;
import io.reign.mesg.ResponseMessage;
import io.reign.util.PathCache;

import java.util.List;

import org.apache.zookeeper.data.ACL;

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

    public ReignContext getContext();

    public void setContext(ReignContext serviceDirectory);

    public void setPathCache(PathCache pathCache);

    public void setDefaultAclList(List<ACL> defaultAclList);

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
     * 
     * @param message
     */
    public ResponseMessage handleMessage(RequestMessage message);

    /**
     * Initialize the service.
     */
    public void init();

    /**
     * Clean up resources as necessary.
     */
    public void destroy();

}
