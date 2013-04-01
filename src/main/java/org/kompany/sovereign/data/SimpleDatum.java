package org.kompany.sovereign.data;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author ypai
 * 
 */
public class SimpleDatum implements Datum {

    private final Map<String, DataItem> datumMap = new HashMap<String, DataItem>(16, 0.9f);

    @Override
    public DataItem getDataItem(String key) {
        return datumMap.get(key);
    }

    @Override
    public void setDataItem(String key, DataItem value) {
        datumMap.put(key, value);
    }
}
