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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;

/**
 * 
 * @author ypai
 * 
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({ "clusterId", "serviceId", "intervalStartTimestamp", "intervalLength", "intervalLengthTimeUnit" })
public class MetricsData {

    private Integer dataNodeCount = 0;

    private Integer dataNodeInWindowCount = 0;

    @JsonProperty("gauges")
    private Map<String, GaugeData> gauges = Collections.EMPTY_MAP;

    @JsonProperty("counters")
    private Map<String, CounterData> counters = Collections.EMPTY_MAP;

    @JsonProperty("histograms")
    private Map<String, HistogramData> histograms = Collections.EMPTY_MAP;

    @JsonProperty("meters")
    private Map<String, MeterData> meters = Collections.EMPTY_MAP;

    @JsonProperty("timers")
    private Map<String, TimerData> timers = Collections.EMPTY_MAP;

    private Long intervalStartTimestamp;

    private Integer intervalLength;

    private TimeUnit intervalLengthUnit;

    private String clusterId;

    private String serviceId;

    private long lastUpdatedTimestamp;

    public long getLastUpdatedTimestamp() {
        return lastUpdatedTimestamp;
    }

    public void setLastUpdatedTimestamp(long lastUpdatedTimestamp) {
        this.lastUpdatedTimestamp = lastUpdatedTimestamp;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public String getServiceId() {
        return serviceId;
    }

    public void setServiceId(String serviceId) {
        this.serviceId = serviceId;
    }

    public void setDataNodeInWindowCount(Integer dataNodeInWindowCount) {
        this.dataNodeInWindowCount = dataNodeInWindowCount;
    }

    public Integer getDataNodeInWindowCount() {
        return dataNodeInWindowCount;
    }

    public void setDataNodeCount(Integer dataNodeCount) {
        this.dataNodeCount = dataNodeCount;
    }

    public Integer getDataNodeCount() {
        return dataNodeCount;
    }

    public Long getIntervalStartTimestamp() {
        return intervalStartTimestamp;
    }

    public void setIntervalStartTimestamp(Long intervalStartTimestamp) {
        this.intervalStartTimestamp = intervalStartTimestamp;
    }

    public Integer getIntervalLength() {
        return intervalLength;
    }

    public void setIntervalLength(Integer intervalLength) {
        this.intervalLength = intervalLength;
    }

    public TimeUnit getIntervalLengthUnit() {
        return intervalLengthUnit;
    }

    public void setIntervalLengthUnit(TimeUnit intervalLengthUnit) {
        this.intervalLengthUnit = intervalLengthUnit;
    }

    public GaugeData getGauge(String key) {
        return gauges.get(key);
    }

    public Map<String, GaugeData> getGauges() {
        return gauges;
    }

    public void setGauges(Map<String, GaugeData> gauges) {
        this.gauges = gauges;
    }

    public CounterData getCounter(String key) {
        return counters.get(key);
    }

    public Map<String, CounterData> getCounters() {
        return counters;
    }

    public void setCounters(Map<String, CounterData> counters) {
        this.counters = counters;
    }

    public HistogramData getHistogram(String key) {
        return histograms.get(key);
    }

    public Map<String, HistogramData> getHistograms() {
        return histograms;
    }

    public void setHistograms(Map<String, HistogramData> histograms) {
        this.histograms = histograms;
    }

    public MeterData getMeter(String key) {
        return meters.get(key);
    }

    public Map<String, MeterData> getMeters() {
        return meters;
    }

    public void setMeters(Map<String, MeterData> meters) {
        this.meters = meters;
    }

    public TimerData getTimer(String key) {
        return timers.get(key);
    }

    public Map<String, TimerData> getTimers() {
        return timers;
    }

    public void setTimers(Map<String, TimerData> timers) {
        this.timers = timers;
    }

}
