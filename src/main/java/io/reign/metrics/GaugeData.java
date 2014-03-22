package io.reign.metrics;

import java.util.List;

/**
 * 
 * @author ypai
 * 
 */
public class GaugeData {
    private double value;

    public double getValue() {
        return value;
    }

    void setValue(double value) {
        this.value = value;
    }

    public static GaugeData merge(List<GaugeData> dataList) {
        int samples = dataList.size();
        double sum = 0;
        for (GaugeData data : dataList) {
            sum += data.getValue();
        }

        GaugeData gaugeData = new GaugeData();
        gaugeData.setValue(sum / samples);
        return gaugeData;
    }
}
