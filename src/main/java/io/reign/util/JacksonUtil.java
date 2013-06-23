package io.reign.util;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * 
 * @author ypai
 * 
 */
public class JacksonUtil {
    /**
     * Reusable Jackson JSON mapper
     */
    private static ObjectMapper DEFAULT_OBJECT_MAPPER = new ObjectMapper();
    static {
        DEFAULT_OBJECT_MAPPER.getDeserializationConfig().without(
                DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);

    }

    public static ObjectMapper getObjectMapperInstance() {
        return DEFAULT_OBJECT_MAPPER;
    }
}
