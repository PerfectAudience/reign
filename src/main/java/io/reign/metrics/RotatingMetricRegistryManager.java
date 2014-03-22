package io.reign.metrics;

import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;

/**
 * 
 * @author ypai
 * 
 */
public class RotatingMetricRegistryManager implements MetricRegistryManager {

    private static final Logger logger = LoggerFactory.getLogger(RotatingMetricRegistryManager.class);

    private volatile MetricRegistry metricRegistry;
    private volatile long lastRotatedTimestamp = 0L;

    private final int rotationInterval;
    private final TimeUnit rotationTimeUnit;
    private final long rotationIntervalMillis;

    public RotatingMetricRegistryManager(int rotationInterval, TimeUnit rotationTimeUnit) {
        this.rotationInterval = rotationInterval;
        this.rotationTimeUnit = rotationTimeUnit;
        rotationIntervalMillis = rotationTimeUnit.toMillis(rotationInterval);
        rotateAsNecessary();
    }

    @Override
    public MetricRegistry get() {
        return this.metricRegistry;
    }

    @Override
    public int getRotationInterval() {
        return this.rotationInterval;
    }

    @Override
    public TimeUnit getRotationTimeUnit() {
        return this.rotationTimeUnit;
    }

    @Override
    public long getLastRotatedTimestamp() {
        return this.lastRotatedTimestamp;
    }

    @Override
    public synchronized void rotateAsNecessary() {
        if (System.currentTimeMillis() - lastRotatedTimestamp > rotationIntervalMillis) {
            this.metricRegistry = new MetricRegistry();
            this.lastRotatedTimestamp = getNormalizedTimestamp(rotationIntervalMillis);
            logger.debug(
                    "Rotating MetricRegistry:  System.currentTimeMillis()={}; lastRotatedTimestamp={}; rotationIntervalMillis={}; lastRotatedTimestamp={}",
                    System.currentTimeMillis(), lastRotatedTimestamp, rotationIntervalMillis, lastRotatedTimestamp);
        }
    }

    long getNormalizedTimestamp(long intervalLength) {
        long currentTimestamp = System.currentTimeMillis();
        long intervalStartTimestamp;

        /***** check interval lengths and create a normalized starting point, so all nodes are on the same interval clock *****/
        if (intervalLength >= 3600000) {
            // interval >= hour, set to previous hour start point
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            cal.setTimeInMillis(currentTimestamp);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            intervalStartTimestamp = cal.getTimeInMillis();
        } else if (intervalLength >= 1800000) {
            // interval >= 30 minutes, set to previous half-hour start point
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            cal.setTimeInMillis(currentTimestamp);

            if (cal.get(Calendar.MINUTE) >= 30) {
                cal.set(Calendar.MINUTE, 30);
            } else {
                cal.set(Calendar.MINUTE, 0);
            }

            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            intervalStartTimestamp = cal.getTimeInMillis();
        } else if (intervalLength >= 900000) {
            // interval >= 15 minutes, set to nearest previous quarter hour
            // start point
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            cal.setTimeInMillis(currentTimestamp);

            int minutes = cal.get(Calendar.MINUTE);
            int diff = minutes % 15;
            cal.set(Calendar.MINUTE, minutes - diff);

            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            intervalStartTimestamp = cal.getTimeInMillis();

        } else if (intervalLength >= 600000) {
            // interval >= 10 minutes, set to nearest previous 10 minute
            // start point
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            cal.setTimeInMillis(currentTimestamp);

            int minutes = cal.get(Calendar.MINUTE);
            int diff = minutes % 10;
            cal.set(Calendar.MINUTE, minutes - diff);

            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            intervalStartTimestamp = cal.getTimeInMillis();

        } else if (intervalLength >= 300000) {
            // interval >= 5 minutes, set to nearest previous 5 minute start
            // point
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            cal.setTimeInMillis(currentTimestamp);

            int minutes = cal.get(Calendar.MINUTE);
            int diff = minutes % 5;
            cal.set(Calendar.MINUTE, minutes - diff);

            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            intervalStartTimestamp = cal.getTimeInMillis();

        } else if (intervalLength >= 60000) {
            // interval >= 1 minute, set to nearest previous minute start point
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            cal.setTimeInMillis(currentTimestamp);
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            intervalStartTimestamp = cal.getTimeInMillis();
        } else {
            // smaller resolutions we just start whenever
            intervalStartTimestamp = currentTimestamp;
        }

        return intervalStartTimestamp;
    }
}
