package io.reign.data;

import io.reign.DataSerializer;

/**
 * Pass-through serializer for byte arrays.
 * 
 * @author ypai
 * 
 */
public class BytesSerializer implements DataSerializer<byte[]> {

    @Override
    public byte[] serialize(byte[] data) throws Exception {
        return data;
    }

    @Override
    public byte[] deserialize(byte[] bytes) throws Exception {
        return bytes;
    }

}
