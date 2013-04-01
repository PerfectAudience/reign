package org.kompany.sovereign.data;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author ypai
 * 
 */
public class NodeDatum implements Datum {

    private final Map<String, SimpleDataItem<?>> datumMap = new HashMap<String, SimpleDataItem<?>>(16, 0.9f);

    @Override
    public <T> SimpleDataItem<T> getDataItem(String key) {
        return (SimpleDataItem<T>) datumMap.get(key);
    }

    @Override
    public <T> void setDataItem(String key, SimpleDataItem<T> value) {
        datumMap.put(key, value);
    }

}
