package io.reign.data;

import io.reign.DataSerializer;

import java.nio.ByteBuffer;

/**
 * 
 * @author ypai
 * 
 */
public class FloatSerializer implements DataSerializer<Float> {
    @Override
    public synchronized byte[] serialize(Float data) throws Exception {
        byte[] bytes = new byte[4];
        ByteBuffer.wrap(bytes).putFloat(data);
        return bytes;
    }

    @Override
    public synchronized Float deserialize(byte[] bytes) throws Exception {
        return ByteBuffer.wrap(bytes).getFloat();

    }
}
