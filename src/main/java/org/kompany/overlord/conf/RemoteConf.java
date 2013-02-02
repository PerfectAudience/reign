package org.kompany.overlord.conf;

/**
 * 
 * @author ypai
 * 
 */
public interface RemoteConf {

    /**
     * 
     * @param key
     * @return
     */
    public String getString(String key);

    /**
     * Optional
     * 
     * @param key
     * @return
     */
    public Object getObject(String key);

    /**
     * Optional
     * 
     * @param key
     * @return
     */
    public Integer getInt(String key);

    /**
     * Optional
     * 
     * @param key
     * @return
     */
    public Long getLong(String key);

    /**
     * Optional
     * 
     * @param key
     * @return
     */
    public Float getFloat(String key);

    /**
     * Optional
     * 
     * @param key
     * @return
     */
    public Double getDouble(String key);

    /**
     * Optional
     * 
     * @param key
     * @return
     */
    public byte[] getBytes(String key);
}
