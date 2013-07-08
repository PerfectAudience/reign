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

package io.reign.data;

import java.util.List;

/**
 * Data stored under a given key that can have one or more values.
 * 
 * @author ypai
 * 
 */
public interface MultiData<V> extends BaseData {

    /**
     * Set value for DataValue.DEFAULT_INDEX
     * 
     * @param value
     */
    public void set(V value);

    /**
     * Set value for a given index
     * 
     * @param index
     * @param value
     */
    public void set(String index, V value);

    /**
     * 
     * @return the value with DataValue.DEFAULT_INDEX
     */
    public V get(Class<V> typeClass);

    public V get(int ttlMillis, Class<V> typeClass);

    /**
     * @param index
     * @return the value with index
     */
    public V get(String index, Class<V> typeClass);

    public V get(String index, int ttlMillis, Class<V> typeClass);

    /**
     * 
     * @return all values
     */
    public List<V> getAll(Class<V> typeClass);

    public List<V> getAll(int ttlMillis, Class<V> typeClass);

    /**
     * Remove value for DataValue.DEFAULT_INDEX
     * 
     * @return
     */
    public String remove();

    public String remove(int ttlMillis);

    /**
     * Remove value at index
     * 
     * @param index
     * @return
     */
    public String remove(String index);

    public String remove(String index, int ttlMillis);

    /**
     * Remove all values associated with this point.
     * 
     * @return
     */
    public List<String> removeAll();

    public List<String> removeAll(int ttlMillis);

}
