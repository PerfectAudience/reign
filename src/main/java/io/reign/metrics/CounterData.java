package io.reign.metrics;

import java.util.List;

/**
 * 
 * @author ypai
 * 
 */
public class CounterData {

    private long count;

    public long getCount() {
        return count;
    }

    void setCount(long count) {
        this.count = count;
    }

    public static CounterData merge(List<CounterData> dataList) {
        long sum = 0;
        for (CounterData data : dataList) {
            sum += data.getCount();
        }
        CounterData counterData = new CounterData();
        counterData.setCount(sum);
        return counterData;
    }

}
