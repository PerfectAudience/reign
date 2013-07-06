package io.reign.data;

import java.util.List;

/**
 * A data point with one or more values.
 * 
 * @author ypai
 * 
 */
public interface PointData<T> {

    /**
     * 
     * @return the value with DataValue.DEFAULT_INDEX
     */
    public DataValue<T> value();

    /**
     * 
     * @return all values
     */
    public List<DataValue<T>> values();
}
