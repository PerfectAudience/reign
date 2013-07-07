package io.reign.data;

import java.util.List;

/**
 * Data stored under a given key that can have one or more values.
 * 
 * @author ypai
 * 
 */
public interface MultiData<T> extends BaseData {

    /**
     * Set value for DataValue.DEFAULT_INDEX
     * 
     * @param value
     */
    public void set(T value);

    /**
     * Set value for a given index
     * 
     * @param index
     * @param value
     */
    public void set(String index, T value);

    /**
     * 
     * @return the value with DataValue.DEFAULT_INDEX
     */
    public T get();

    public T get(int ttlMillis);

    /**
     * @param index
     * @return the value with index
     */
    public T get(String index);

    public T get(String index, int ttlMillis);

    /**
     * 
     * @return all values
     */
    public List<T> getAll();

    public List<T> getAll(int ttlMillis);

    /**
     * Remove value for DataValue.DEFAULT_INDEX
     * 
     * @return
     */
    public String remove();

    public String remove(int ttlMillis);

    /**
     * Remove value at index
     * 
     * @param index
     * @return
     */
    public String remove(String index);

    public String remove(String index, int ttlMillis);

    /**
     * Remove all values associated with this point.
     * 
     * @return
     */
    public List<String> removeAll();

    public List<String> removeAll(int ttlMillis);

}
