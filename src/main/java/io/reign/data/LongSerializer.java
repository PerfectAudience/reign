package io.reign.data;

import io.reign.DataSerializer;

import java.nio.ByteBuffer;

/**
 * 
 * @author ypai
 * 
 */
public class LongSerializer implements DataSerializer<Long> {

    @Override
    public synchronized byte[] serialize(Long data) throws Exception {
        byte[] bytes = new byte[8];
        ByteBuffer.wrap(bytes).putLong(data);
        return bytes;
    }

    @Override
    public synchronized Long deserialize(byte[] bytes) throws Exception {
        return ByteBuffer.wrap(bytes).getLong();

    }

}
