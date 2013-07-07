package io.reign.data;

import io.reign.DataSerializer;

import java.nio.ByteBuffer;

/**
 * 
 * @author ypai
 * 
 */
public class DoubleSerializer implements DataSerializer<Double> {

    @Override
    public synchronized byte[] serialize(Double data) throws Exception {
        byte[] bytes = new byte[8];
        ByteBuffer.wrap(bytes).putDouble(data);
        return bytes;
    }

    @Override
    public synchronized Double deserialize(byte[] bytes) throws Exception {
        return ByteBuffer.wrap(bytes).getDouble();

    }

}
