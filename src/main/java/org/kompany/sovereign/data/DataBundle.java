package org.kompany.sovereign.data;

/**
 * 
 * @author ypai
 * 
 */
public interface DataBundle {

    public <T extends DataPoint<?>> T getDataPoint(String key);

    public void setDataPoint(String key, DataPoint<?> value);

}
