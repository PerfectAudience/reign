package org.kompany.overlord;

/**
 * Wrapper class so that services can look-up other services without having full
 * access to Sovereign object.
 * 
 * @author ypai
 * 
 */
public interface ServiceDirectory {

    public Service getService(String serviceName);

}
