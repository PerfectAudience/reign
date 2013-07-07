package io.reign.data;

import io.reign.DataSerializer;

import java.nio.ByteBuffer;

/**
 * 
 * @author ypai
 * 
 */
public class IntegerSerializer implements DataSerializer<Integer> {

    @Override
    public synchronized byte[] serialize(Integer data) throws Exception {
        byte[] bytes = new byte[4];
        ByteBuffer.wrap(bytes).putInt(data);
        return bytes;
    }

    @Override
    public synchronized Integer deserialize(byte[] bytes) throws Exception {
        return ByteBuffer.wrap(bytes).getInt();

    }
}
