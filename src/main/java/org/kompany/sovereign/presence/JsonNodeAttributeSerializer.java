package org.kompany.sovereign.presence;

import java.util.Map;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

/**
 * 
 * @author ypai
 * 
 */
public class JsonNodeAttributeSerializer implements NodeAttributeSerializer {
    /**
     * Reusable Jackson JSON mapper
     */
    private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static {
        OBJECT_MAPPER.getDeserializationConfig().without(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);

    }

    @Override
    public byte[] serialize(Map<String, String> map) throws Exception {
        return OBJECT_MAPPER.writeValueAsString(map).getBytes("UTF-8");

    }

    @Override
    public Map<String, String> deserialize(byte[] bytes) throws Exception {
        return OBJECT_MAPPER.readValue(bytes, 0, bytes.length, new TypeReference<Map<String, String>>() {
        });
    }

}
