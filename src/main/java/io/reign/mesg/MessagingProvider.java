package io.reign.mesg;

import io.reign.ReignContext;

/**
 * 
 * @author ypai
 * 
 */
public interface MessagingProvider {

    /**
     * Automatically set during bootstrapping.
     * 
     * @param serviceDirectory
     */
    public void setServiceDirectory(ReignContext serviceDirectory);

    public String sendMessage(String hostOrIpAddress, int port, String message);

    public byte[] sendMessage(String hostOrIpAddress, int port, byte[] message);

    public String sendMessageForget(String hostOrIpAddress, int port, String message);

    public byte[] sendMessageForget(String hostOrIpAddress, int port, byte[] message);

    public void setMessageProtocol(MessageProtocol messageProtocol);

    public MessageProtocol getMessageProtocol();

    public void setPort(int port);

    public int getPort();

    public void init();

    public void destroy();

}
