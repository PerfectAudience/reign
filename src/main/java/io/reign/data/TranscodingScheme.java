package io.reign.data;

/**
 * Abstraction for serialization scheme used.
 * 
 * @author ypai
 * 
 */
public interface TranscodingScheme {

    public byte[] toBytes(Object value);

    public <T> T fromBytes(byte[] bytes, Class clazz);
}
