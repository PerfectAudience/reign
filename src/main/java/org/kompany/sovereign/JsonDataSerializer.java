package org.kompany.sovereign;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class JsonDataSerializer<T> implements DataSerializer<T> {

    /**
     * Reusable Jackson JSON mapper
     */
    private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static {
        OBJECT_MAPPER.getDeserializationConfig().without(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);

    }

    @Override
    public byte[] serialize(T data) throws Exception {
        return OBJECT_MAPPER.writeValueAsString(data).getBytes("UTF-8");
    }

    @Override
    public T deserialize(byte[] bytes) throws Exception {
        return OBJECT_MAPPER.readValue(bytes, 0, bytes.length, new TypeReference<T>() {
        });
    }

}
