package io.reign.data;

import java.util.List;

/**
 * Data stored under a given key that can have one or more values.
 * 
 * @author ypai
 * 
 */
public interface MultiData<T> {

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

    /**
     * 
     * @return all values
     */
    public List<T> getAll();

    /**
     * Remove value for DataValue.DEFAULT_INDEX
     * 
     * @return
     */
    public String remove();

    /**
     * Remove all values associated with this point.
     * 
     * @return
     */
    public List<String> removeAll();
}
