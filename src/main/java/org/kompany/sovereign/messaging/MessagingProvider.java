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

    public String sendMessage(String hostOrIpAddress, int port, String message);

    public byte[] sendMessage(String hostOrIpAddress, int port, byte[] message);

    public void setMessageProtocol(MessageProtocol messageProtocol);

    public MessageProtocol getMessageProtocol();

    public void setPort(int port);

    public int getPort();

    public void start();

    public void stop();

}
