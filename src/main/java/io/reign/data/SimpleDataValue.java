package io.reign.data;

/**
 * 
 * @author ypai
 * 
 */
public class SimpleDataValue<T> implements DataValue<T> {

    private String index = DEFAULT_INDEX;

    private long lastModified;
    private long timeToLive;

    private T value;

    public SimpleDataValue(String index, T value, long timeToLive, long lastModified) {
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
    public long getTimeToLive() {
        return timeToLive;
    }

    public void setTimeToLive(long timeToLive) {
        this.timeToLive = timeToLive;
    }

}
