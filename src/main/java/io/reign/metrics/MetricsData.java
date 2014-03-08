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

package io.reign.metrics;

import java.util.Map;

/**
 * 
 * @author ypai
 * 
 */
public class MetricsData {

    private Map<String, Object> gauges;

    private Map<String, Object> counters;

    private Map<String, Object> histograms;

    private Map<String, Object> meters;

    private Map<String, Object> timers;

    public Map<String, Object> getGauges() {
        return gauges;
    }

    public void setGauges(Map<String, Object> gauges) {
        this.gauges = gauges;
    }

    public Map<String, Object> getCounters() {
        return counters;
    }

    public void setCounters(Map<String, Object> counters) {
        this.counters = counters;
    }

    public Map<String, Object> getHistograms() {
        return histograms;
    }

    public void setHistograms(Map<String, Object> histograms) {
        this.histograms = histograms;
    }

    public Map<String, Object> getMeters() {
        return meters;
    }

    public void setMeters(Map<String, Object> meters) {
        this.meters = meters;
    }

    public Map<String, Object> getTimers() {
        return timers;
    }

    public void setTimers(Map<String, Object> timers) {
        this.timers = timers;
    }

}
