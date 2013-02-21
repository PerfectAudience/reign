package org.kompany.overlord;

/**
 * Wrapper class so that services can look-up other services.
 * 
 * @author ypai
 * 
 */
public interface ServiceDirectory {

    public Service getService(String serviceName);

}
