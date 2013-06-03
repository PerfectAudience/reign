package org.kompany.sovereign;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.kompany.sovereign.util.JacksonUtil;

/**
 * 
 * @author ypai
 * 
 * @param <T>
 */
public class JsonDataSerializer<T> implements DataSerializer<T> {

    /**
     * Reusable Jackson JSON mapper
     */
    private static ObjectMapper OBJECT_MAPPER = JacksonUtil.getObjectMapperInstance();

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
