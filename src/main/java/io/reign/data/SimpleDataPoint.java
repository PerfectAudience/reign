package io.reign.data;

/**
 * 
 * @author ypai
 * 
 */
public class SimpleDataPoint<T> implements DataPoint<T> {

    private String key;

    private Aggregation aggregation;

    private Integer aggregationIntervalSecs;

    private T value;

    public SimpleDataPoint(String key, T value, Aggregation aggregation, Integer aggregationIntervalSecs) {
        super();
        setKey(key);
        this.value = value;
        setAggregationIntervalSecs(aggregationIntervalSecs);
        setAggregation(aggregation);
    }

    @Override
    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    @Override
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public Aggregation getAggregation() {
        return aggregation;
    }

    public void setAggregation(Aggregation aggregation) {
        this.aggregation = aggregation;
    }

    @Override
    public Integer getAggregationIntervalSecs() {
        return aggregationIntervalSecs;
    }

    public void setAggregationIntervalSecs(Integer aggregationIntervalSecs) {
        this.aggregationIntervalSecs = aggregationIntervalSecs;
    }
}
