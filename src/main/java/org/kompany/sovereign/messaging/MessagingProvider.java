package org.kompany.sovereign.messaging;

import org.kompany.sovereign.ServiceDirectory;

/**
 * 
 * @author ypai
 * 
 */
public interface MessagingProvider {

    public MessageQueue getRequestQueue();

    public MessageQueue getResponseQueue();

    /**
     * Automatically set during bootstrapping.
     * 
     * @param serviceDirectory
     */
    public void setServiceDirectory(ServiceDirectory serviceDirectory);

    public void setPort(int port);

    public void start();

    public void stop();

}
