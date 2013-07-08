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

import java.util.concurrent.TimeUnit;

/**
 * 
 * @author ypai
 * 
 */
public class TimeUnitUtil {

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
