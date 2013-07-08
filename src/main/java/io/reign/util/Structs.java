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

package io.reign.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Properties;

/**
 * Provides some helpful util classes for more concise code.
 * 
 * @author ypai
 * 
 */
public class Structs {

    /**
     * 
     * @return a List that allows chaining when setting values.
     */
    public static <V> Iterable<V> iterable(V[] array) {
        Iterable<V> iterable = new ArrayIterable<V>(array);
        return iterable;
    }

    public static class ArrayIterable<V> implements Iterable<V> {
        private final V[] array;

        public ArrayIterable(V[] array) {
            this.array = array;
        }

        @Override
        public Iterator<V> iterator() {
            return new Iterator<V>() {

                private int index = -1;

                @Override
                public boolean hasNext() {
                    return index < array.length - 1;
                }

                @Override
                public V next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException("No more elements!");
                    }
                    index++;
                    return array[index];
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("remove() is not supported!");
                }

            };
        }

    }

    /**
     * 
     * @return a List that allows chaining when setting values.
     */
    public static <V> BuildableList<V> list() {
        BuildableList<V> map = new BuildableList<V>();
        return map;
    }

    public static class BuildableList<V> extends ArrayList<V> {
        public BuildableList<V> v(V val) {
            this.add(val);
            return this;
        }
    }

    /**
     * 
     * @return a Map that allows chaining when setting values.
     */
    public static <K, V> BuildableMap<K, V> map() {
        BuildableMap<K, V> map = new BuildableMap<K, V>();
        return map;
    }

    public static class BuildableMap<K, V> extends HashMap<K, V> {
        public BuildableMap<K, V> kv(K key, V val) {
            this.put(key, val);
            return this;
        }
    }

    /**
     * 
     * @return a Properties object that allows chaining when setting properties.
     */
    public static BuildableProperties properties() {
        BuildableProperties map = new BuildableProperties();
        return map;
    }

    public static class BuildableProperties extends Properties {
        public BuildableProperties kv(String key, String val) {
            this.setProperty(key, val);
            return this;
        }
    }
}
