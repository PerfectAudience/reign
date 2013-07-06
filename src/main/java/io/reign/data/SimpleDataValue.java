package io.reign.data;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * 
 * @author ypai
 * 
 */
public class SimpleDataValue<T> implements DataValue<T> {

    @JsonIgnore
    private String index = DEFAULT_INDEX;

    @JsonProperty("m")
    private long lastModified;

    @JsonProperty("t")
    private int timeToLive;

    @JsonProperty("v")
    private T value;

    public SimpleDataValue(String index, T value, int timeToLive, long lastModified) {
        super();
        this.index = index;
        this.value = value;
        this.timeToLive = timeToLive;
        this.lastModified = lastModified;

    }

    @Override
    public String getIndex() {
        return this.index;
    }

    @Override
    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    @Override
    public long getLastModified() {
        return lastModified;
    }

    public void setLastModified(long lastModified) {
        this.lastModified = lastModified;
    }

    @Override
    public int getTimeToLive() {
        return timeToLive;
    }

    public void setTimeToLive(int timeToLive) {
        this.timeToLive = timeToLive;
    }

}
