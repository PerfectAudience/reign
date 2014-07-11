package io.reign.metrics;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 
 * @author ypai
 * 
 */
public class TimerData {

	private long count;
	private double max;
	private double mean;
	private double min;
	private double stddev;
	private double p50;
	private double p75;
	private double p95;
	private double p98;
	private double p99;
	private double p999;

	private double meanRate;

	private double m1Rate;

	private double m5Rate;

	private double m15Rate;

	private TimeUnit rateUnit;

	private TimeUnit durationUnit;

	public static TimerData merge(List<TimerData> dataList) {
		long samples = 0;
		double meanSum = 0;
		double min = Double.MAX_VALUE;
		double max = Double.MIN_VALUE;

		double stddevSum = 0;

		double p50Sum = 0;
		double p75Sum = 0;
		double p95Sum = 0;
		double p98Sum = 0;
		double p99Sum = 0;
		double p999Sum = 0;

		for (TimerData data : dataList) {
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

		TimerData data = new TimerData();
		data.setCount(samples);
		data.setMin(samples > 0 ? min : 0);
		data.setMax(samples > 0 ? max : 0);

		if (dataList.size() > 0) {
			data.setRateUnit(dataList.get(0).getRateUnit());
			data.setDurationUnit(dataList.get(0).getDurationUnit());
		}

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

	public double getMax() {
		return max;
	}

	public void setMax(double max) {
		this.max = max;
	}

	public double getMean() {
		return mean;
	}

	public void setMean(double mean) {
		this.mean = mean;
	}

	public double getMin() {
		return min;
	}

	public void setMin(double min) {
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

	public TimeUnit getDurationUnit() {
		return durationUnit;
	}

	public void setDurationUnit(TimeUnit durationUnit) {
		this.durationUnit = durationUnit;
	}

}
