package io.reign.data;

/**
 * 
 * @author ypai
 * 
 */
public interface DataValue<T> {

    public static final String DEFAULT_INDEX = "0";

    /**
     * For a data node that has multiple values, this unique index is used to distinguish one value from another in
     * storage.
     * 
     * @return
     */
    public String getIndex();

    /**
     * 
     * @return last modified timestamp in millis
     */
    public long getLastModified();

    /**
     * 
     * @return the value
     */
    public T getValue();

    /**
     * 
     * @return time to live in millis
     */
    public int getTimeToLive();

}
