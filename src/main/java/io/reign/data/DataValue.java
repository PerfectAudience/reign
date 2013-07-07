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
    public String index();

    /**
     * 
     * @return last modified timestamp in millis
     */
    public long lastModified();

    public T value();

}
