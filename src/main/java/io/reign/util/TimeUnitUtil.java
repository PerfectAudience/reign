/*
 Copyright 2013 Yen Pai ypai@reign.io

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package io.reign.util;

import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * 
 * @author ypai
 * 
 */
public class TimeUnitUtil {

    // public static long toMillis(long interval, TimeUnit timeUnit) {
    // // convert wait to millis
    // long timeWaitMillis = 0;
    // switch (timeUnit) {
    // case DAYS:
    // timeWaitMillis = interval * 86400 * 1000;
    // break;
    // case HOURS:
    // timeWaitMillis = interval * 3600 * 1000;
    // break;
    // case MINUTES:
    // timeWaitMillis = interval * 60 * 1000;
    // break;
    // case SECONDS:
    // timeWaitMillis = interval * 1000;
    // break;
    // case MILLISECONDS:
    // timeWaitMillis = interval;
    // break;
    // default:
    // // anything less than millis, assume to be 0
    // }
    // return timeWaitMillis;
    // }

    public static long getNormalizedIntervalStartTimestamp(long intervalLength, TimeUnit intervalLengthTimeUnit,
            long currentTimestamp) {
        return getNormalizedIntervalStartTimestamp(intervalLengthTimeUnit.toMillis(intervalLength), currentTimestamp);
    }

    public static long getNormalizedIntervalStartTimestamp(long intervalLengthMillis, long currentTimestamp) {

        long intervalStartTimestamp;

        /***** check interval lengths and create a normalized starting point, so all nodes are on the same interval clock *****/
        if (intervalLengthMillis >= 3600000) {
            // interval >= hour, set to previous hour start point
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            cal.setTimeInMillis(currentTimestamp);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            intervalStartTimestamp = cal.getTimeInMillis();
        } else if (intervalLengthMillis >= 1800000) {
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
        } else if (intervalLengthMillis >= 900000) {
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

        } else if (intervalLengthMillis >= 600000) {
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

        } else if (intervalLengthMillis >= 300000) {
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

        } else if (intervalLengthMillis >= 60000) {
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
