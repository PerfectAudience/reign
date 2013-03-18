package org.kompany.sovereign;

/**
 * Wrapper class so that services can look-up other services without having full
 * access to Sovereign object.
 * 
 * @author ypai
 * 
 */
public interface ServiceDirectory {

    public <T extends Service> T getService(String serviceName);

}
