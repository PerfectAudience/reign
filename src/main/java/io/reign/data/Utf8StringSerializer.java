package io.reign.data;

import io.reign.DataSerializer;

/**
 * Pass-through serializer for byte arrays.
 * 
 * @author ypai
 * 
 */
public class Utf8StringSerializer implements DataSerializer<String> {

    @Override
    public byte[] serialize(String data) throws Exception {
        return data.getBytes("UTF-8");
    }

    @Override
    public String deserialize(byte[] bytes) throws Exception {
        return new String(bytes, "UTF-8");
    }

}
