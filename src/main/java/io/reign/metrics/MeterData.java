package io.reign.metrics;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * 
 * @author ypai
 * 
 */
public class MeterData {

    private long count;

    @JsonProperty("mean_rate")
    private double meanRate;

    @JsonProperty("m1_rate")
    private double m1Rate;

    @JsonProperty("m5_rate")
    private double m5Rate;

    @JsonProperty("m15_rate")
    private double m15Rate;

    @JsonProperty("rate_unit")
    private TimeUnit rateUnit;

    public static MeterData merge(List<MeterData> dataList) {
        int samples = 0;
        double meanRateSum = 0;
        double m1RateSum = 0;
        double m5RateSum = 0;
        double m15RateSum = 0;
        for (MeterData data : dataList) {
            meanRateSum += (data.getMeanRate() * data.getCount());
            m1RateSum += (data.getM1Rate() * data.getCount());
            m5RateSum += (data.getM5Rate() * data.getCount());
            m15RateSum += (data.getM15Rate() * data.getCount());
            samples += data.getCount();
        }

        MeterData meterData = new MeterData();
        meterData.setCount(samples);
        meterData.setMeanRate(meanRateSum / samples);
        meterData.setM1Rate(m1RateSum / samples);
        meterData.setM5Rate(m5RateSum / samples);
        meterData.setM15Rate(m15RateSum / samples);

        return meterData;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getMeanRate() {
        return meanRate;
    }

    public void setMeanRate(double meanRate) {
        this.meanRate = meanRate;
    }

    public double getM1Rate() {
        return m1Rate;
    }

    public void setM1Rate(double m1Rate) {
        this.m1Rate = m1Rate;
    }

    public double getM5Rate() {
        return m5Rate;
    }

    public void setM5Rate(double m5Rate) {
        this.m5Rate = m5Rate;
    }

    public double getM15Rate() {
        return m15Rate;
    }

    public void setM15Rate(double m15Rate) {
        this.m15Rate = m15Rate;
    }

    public TimeUnit getRateUnit() {
        return rateUnit;
    }

    public void setRateUnit(TimeUnit rateUnit) {
        this.rateUnit = rateUnit;
    }

}
