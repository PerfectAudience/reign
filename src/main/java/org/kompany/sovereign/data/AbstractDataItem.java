package org.kompany.sovereign.data;

/**
 * 
 * @author ypai
 * 
 */
public abstract class AbstractDataItem<T> implements DataItem<T> {

    private String key;

    private Aggregation aggregation;

    private Integer aggregationIntervalSecs;

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
