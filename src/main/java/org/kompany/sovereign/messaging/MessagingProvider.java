package org.kompany.sovereign.messaging;

import org.kompany.sovereign.ServiceDirectory;

/**
 * 
 * @author ypai
 * 
 */
public interface MessagingProvider {

    // public MessageQueue getRequestQueue();
    //
    // public MessageQueue getResponseQueue();

    /**
     * Automatically set during bootstrapping.
     * 
     * @param serviceDirectory
     */
    public void setServiceDirectory(ServiceDirectory serviceDirectory);

    public ResponseMessage sendMessage(String hostOrIpAddress, int port, RequestMessage message);

    public void setPort(int port);

    public int getPort();

    public void start();

    public void stop();

}
