package io.reign.data;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.reign.DataSerializer;

/**
 * 
 * @author ypai
 * 
 */
public class SimpleTranscodingScheme implements TranscodingScheme {

    private final Map<Class, DataSerializer> dataSerializerMap = new ConcurrentHashMap<Class, DataSerializer>(33, 0.9f,
            1);

    public SimpleTranscodingScheme() {
        // register default serializers
        dataSerializerMap.put(Long.class, new LongSerializer());
        dataSerializerMap.put(Integer.class, new IntegerSerializer());
        dataSerializerMap.put(Float.class, new FloatSerializer());
        dataSerializerMap.put(Double.class, new DoubleSerializer());
        dataSerializerMap.put(Boolean.class, new BooleanSerializer());
        dataSerializerMap.put(Short.class, new ShortSerializer());
        dataSerializerMap.put(Byte.class, new ByteSerializer());
        dataSerializerMap.put(byte[].class, new BytesSerializer());
        dataSerializerMap.put(String.class, new Utf8StringSerializer());
    }

    @Override
    public byte[] toBytes(Object value) {
        if (value == null)
            return null;

        try {
            DataSerializer transcoder = dataSerializerMap.get(value.getClass());
            if (transcoder == null) {
                throw new IllegalStateException("No transcoder found for " + value.getClass());
            }
            return transcoder.serialize(value);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public <T> T fromBytes(byte[] bytes, Class clazz) {
        if (bytes == null || bytes.length == 0)
            return null;

        try {
            DataSerializer transcoder = dataSerializerMap.get(clazz);
            if (transcoder == null) {
                throw new IllegalStateException("No transcoder found for " + clazz);
            }
            return (T) transcoder.deserialize(bytes);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public void register(Class clazz, DataSerializer dataSerializer) {
        this.dataSerializerMap.put(clazz, dataSerializer);
    }

}
