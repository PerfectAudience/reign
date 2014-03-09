package io.reign.metrics;

import java.util.concurrent.TimeUnit;

/**
 * 
 * @author ypai
 * 
 */
public class MeterData {

    private long count;
    private double meanRate;
    private double m1Rate;
    private double m5Rate;
    private double m15Rate;
    private TimeUnit rateUnit;

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
