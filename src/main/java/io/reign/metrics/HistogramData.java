package io.reign.metrics;

import java.util.List;

/**
 * 
 * @author ypai
 * 
 */
public class HistogramData {

    private long count;
    private long max;
    private double mean;
    private long min;
    private double stddev;
    private double p50;
    private double p75;
    private double p95;
    private double p98;
    private double p99;
    private double p999;

    public static HistogramData merge(List<HistogramData> dataList) {
        int samples = 0;
        double meanSum = 0;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;

        double stddevSum = 0;

        double p50Sum = 0;
        double p75Sum = 0;
        double p95Sum = 0;
        double p98Sum = 0;
        double p99Sum = 0;
        double p999Sum = 0;

        for (HistogramData data : dataList) {
            samples += data.getCount();
            meanSum += data.getMean() * data.getCount();
            min = Math.min(data.getMin(), min);
            max = Math.max(data.getMax(), max);

            stddevSum += Math.pow(data.getStddev(), 2);

            p50Sum += data.getP50() * data.getCount();
            p75Sum += data.getP75() * data.getCount();
            p95Sum += data.getP95() * data.getCount();
            p98Sum += data.getP98() * data.getCount();
            p99Sum += data.getP99() * data.getCount();
            p999Sum += data.getP999() * data.getCount();
        }

        HistogramData data = new HistogramData();
        data.setCount(samples);
        data.setMin(samples > 0 ? min : 0);
        data.setMax(samples > 0 ? max : 0);

        // sqrt of variances
        data.setStddev(Math.sqrt(stddevSum));

        // weighted avgs
        if (samples > 0) {
            data.setMean(meanSum / samples);
            data.setP50(p50Sum / samples);
            data.setP75(p75Sum / samples);
            data.setP95(p95Sum / samples);
            data.setP98(p98Sum / samples);
            data.setP99(p99Sum / samples);
            data.setP999(p999Sum / samples);
        }

        return data;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getMax() {
        return max;
    }

    public void setMax(long max) {
        this.max = max;
    }

    public double getMean() {
        return mean;
    }

    public void setMean(double mean) {
        this.mean = mean;
    }

    public long getMin() {
        return min;
    }

    public void setMin(long min) {
        this.min = min;
    }

    public double getStddev() {
        return stddev;
    }

    public void setStddev(double stddev) {
        this.stddev = stddev;
    }

    public double getP50() {
        return p50;
    }

    public void setP50(double p50) {
        this.p50 = p50;
    }

    public double getP75() {
        return p75;
    }

    public void setP75(double p75) {
        this.p75 = p75;
    }

    public double getP95() {
        return p95;
    }

    public void setP95(double p95) {
        this.p95 = p95;
    }

    public double getP98() {
        return p98;
    }

    public void setP98(double p98) {
        this.p98 = p98;
    }

    public double getP99() {
        return p99;
    }

    public void setP99(double p99) {
        this.p99 = p99;
    }

    public double getP999() {
        return p999;
    }

    public void setP999(double p999) {
        this.p999 = p999;
    }

}
