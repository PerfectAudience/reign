package io.reign.util;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;

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
        DEFAULT_OBJECT_MAPPER.setSerializationInclusion(Inclusion.NON_NULL);

    }

    public static ObjectMapper getObjectMapperInstance() {
        return DEFAULT_OBJECT_MAPPER;
    }
}
