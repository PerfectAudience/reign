package org.kompany.overlord.util;

import java.util.concurrent.TimeUnit;

/**
 * 
 * @author ypai
 * 
 */
public class TimeUnitUtils {

    public static long toMillis(long interval, TimeUnit timeUnit) {
        // convert wait to millis
        long timeWaitMillis = 0;
        switch (timeUnit) {
        case DAYS:
            timeWaitMillis = interval * 86400 * 1000;
            break;
        case HOURS:
            timeWaitMillis = interval * 3600 * 1000;
            break;
        case MINUTES:
            timeWaitMillis = interval * 60 * 1000;
            break;
        case SECONDS:
            timeWaitMillis = interval * 1000;
            break;
        case MILLISECONDS:
            timeWaitMillis = interval;
            break;
        default:
            // anything less than millis, assume to be 0
        }
        return timeWaitMillis;
    }
}
