package io.reign.data;

import io.reign.DataSerializer;

/**
 * Pass-through serializer for byte arrays.
 * 
 * @author ypai
 * 
 */
public class ByteSerializer implements DataSerializer<Byte> {

    @Override
    public byte[] serialize(Byte data) throws Exception {
        return new byte[] { data };
    }

    @Override
    public Byte deserialize(byte[] bytes) throws Exception {
        return bytes[0];
    }

}
