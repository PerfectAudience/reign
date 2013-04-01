package org.kompany.sovereign.data;

/**
 * 
 * @author ypai
 * 
 */
public interface DataItem<T> {

    public String getKey();

    public T getValue();

    public Aggregation getAggregation();
}
