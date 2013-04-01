package org.kompany.sovereign.data;

/**
 * 
 * @author ypai
 * 
 */
public interface Datum {

    public <T> SimpleDataItem<T> getDataItem(String key);

    public <T> void setDataItem(String key, SimpleDataItem<T> value);

}
