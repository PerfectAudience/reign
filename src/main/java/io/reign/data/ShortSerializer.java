package io.reign.data;

import io.reign.DataSerializer;

import java.nio.ByteBuffer;

/**
 * 
 * @author ypai
 * 
 */
public class ShortSerializer implements DataSerializer<Short> {
    @Override
    public synchronized byte[] serialize(Short data) throws Exception {
        byte[] bytes = new byte[2];
        ByteBuffer.wrap(bytes).putShort(data);
        return bytes;
    }

    @Override
    public synchronized Short deserialize(byte[] bytes) throws Exception {
        return ByteBuffer.wrap(bytes).getShort();

    }
}
