package io.reign.data;

import org.codehaus.jackson.annotate.JsonIgnore;

/**
 * 
 * @author ypai
 * 
 */
public class ZkDataValue<T> implements DataValue<T> {

    @JsonIgnore
    private String index = DEFAULT_INDEX;

    private final long lastModified;

    private final T value;

    public ZkDataValue(String index, T value, long lastModified) {
        this.index = index;
        this.value = value;
        this.lastModified = lastModified;

    }

    public ZkDataValue(T value, long lastModified) {
        this.value = value;
        this.lastModified = lastModified;

    }

    public ZkDataValue(T value) {
        this.value = value;
        this.lastModified = -1;
    }

    @Override
    public String index() {
        return this.index;
    }

    @Override
    public T value() {
        return value;
    }

    @Override
    public long lastModified() {
        return lastModified;
    }

}
