package org.kompany.sovereign;

/**
 * 
 * @author ypai
 * 
 */
public interface MessagingProvider {

    public void setPort(int port);

    public void start();

    public void stop();

}
