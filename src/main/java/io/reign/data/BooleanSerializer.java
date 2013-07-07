package io.reign.data;

import io.reign.DataSerializer;

/**
 * 
 * @author ypai
 * 
 */
public class BooleanSerializer implements DataSerializer<Boolean> {

    private static final byte TRUE = 1;
    private static final byte FALSE = 0;

    @Override
    public byte[] serialize(Boolean data) throws Exception {
        return new byte[] { data ? TRUE : FALSE };
    }

    @Override
    public Boolean deserialize(byte[] bytes) throws Exception {
        Byte byteObj = new Byte(bytes[0]);
        return byteObj.byteValue() == TRUE;
    }

}
