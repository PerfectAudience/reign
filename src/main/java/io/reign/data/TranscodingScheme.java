package io.reign.data;

import java.io.ByteArrayOutputStream;

import com.esotericsoftware.kryo.io.Output;

import io.reign.DataSerializer;

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
