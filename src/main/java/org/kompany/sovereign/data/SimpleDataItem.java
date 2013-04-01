package org.kompany.sovereign.data;

/**
 * 
 * @author ypai
 * 
 */
public class SimpleDataItem<T> extends AbstractDataItem<T> {

    private T value;

    public SimpleDataItem(String key, T value, Aggregation aggregation) {
        super();
        setKey(key);
        this.value = value;
        setAggregation(aggregation);
    }

    @Override
    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

}
