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
 * A collection of data points organized like a multimap, where keys can have multiple values. A map is assumed to be
 * able to hold multiple data types under different keys, though values under the same key are assumed to be of the same
 * type.
 * 
 * @author ypai
 * 
 */
public interface MultiMapData<K> extends BaseData {

    /**
     * Put value using custom index
     * 
     * @param key
     * @param value
     */
    public <V> void put(K key, String index, V value);

    /**
     * Put value using DataValue.DEFAULT_INDEX
     * 
     * @param key
     * @param value
     */
    public <V> void put(K key, V value);

    /**
     * Get a single value for key under DataValue.DEFAULT_INDEX; it is possible for value to be null.
     * 
     * @param <T>
     * @param key
     * @param typeClass
     * @return
     */
    public <V> V get(K key, Class<V> typeClass);

    /**
     * Get a single value for key for given index; it is possible for value to be null.
     * 
     * @param <T>
     * @param key
     * @param typeClass
     * @return
     */
    public <V> V get(K key, String index, Class<V> typeClass);

    /**
     * Get all values for key; it is possible for values to be null.
     * 
     * @param <T>
     * @param key
     * @param typeClass
     * @return
     */
    public <V, T extends List<V>> T getAll(K key, Class<V> typeClass);

    /**
     * Remove value under key with index DataValue.DEFAULT_INDEX
     * 
     * @param key
     * @return
     */
    public String remove(K key);

    /**
     * Remove a specific value for a given key
     * 
     * @param key
     * @param index
     * @return
     */
    public String remove(K key, String index);

    /**
     * Remove all values under key
     * 
     * @param key
     * @return
     */
    public List<String> removeAll(K key);

    /**
     * @return number of keys
     */
    public int size();

    /**
     * 
     * @return List of available keys
     */
    public List<String> keys();

}
