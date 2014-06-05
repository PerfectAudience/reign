package io.reign.metrics;

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

import io.reign.AbstractObserver;
import io.reign.ReignException;
import io.reign.util.JacksonUtil;

import java.util.List;

/**
 * 
 * @author ypai
 * 
 */
public abstract class MetricsObserver extends AbstractObserver {

    public abstract void updated(MetricsData updated, MetricsData previous);

    @Override
    public void nodeDataChanged(byte[] updatedData, byte[] previousData) {
        MetricsData previous = toMetricsData(previousData);
        MetricsData updated = toMetricsData(updatedData);
        updated(updated, previous);
    }

    @Override
    public void nodeDeleted(byte[] previousData, List<String> previousChildList) {
        MetricsData previous = toMetricsData(previousData);
        updated(null, previous);
    }

    @Override
    public void nodeCreated(byte[] data, List<String> childList) {
        if (data != null) {
            MetricsData updated = toMetricsData(data);
            updated(updated, null);
        }
    }

    MetricsData toMetricsData(byte[] bytes) {
        if (bytes == null)
            return null;
        try {
            MetricsData metricsData = JacksonUtil.getObjectMapper().readValue(bytes, MetricsData.class);
            return metricsData;
        } catch (Exception e) {
            throw new ReignException(e);
        }
    }

}
