package org.kompany.sovereign.data;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author ypai
 * 
 */
public class SimpleDataBundle implements DataBundle {

    private final Map<String, DataPoint<?>> datumMap = new HashMap<String, DataPoint<?>>(16, 0.9f);

    @Override
    public DataPoint<?> getDataPoint(String key) {
        return datumMap.get(key);
    }

    @Override
    public void setDataPoint(String key, DataPoint<?> value) {
        datumMap.put(key, value);
    }
}
