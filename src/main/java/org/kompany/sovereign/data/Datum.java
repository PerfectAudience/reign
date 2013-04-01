package org.kompany.sovereign.data;

/**
 * 
 * @author ypai
 * 
 */
public interface Datum {

    public <T extends DataItem<?>> T getDataItem(String key);

    public void setDataItem(String key, DataItem<?> value);

}
