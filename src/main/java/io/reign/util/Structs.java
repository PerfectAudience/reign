package io.reign.util;

import java.util.HashMap;
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
