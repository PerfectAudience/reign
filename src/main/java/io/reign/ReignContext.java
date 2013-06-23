package io.reign;

/**
 * Provides access to essential framework capabilities without exposing base framework object.
 * 
 * @author ypai
 * 
 */
public interface ReignContext {

    public <T extends Service> T getService(String serviceName);

    public String getReservedClusterId();
}
