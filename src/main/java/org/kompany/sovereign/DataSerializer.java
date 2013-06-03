package org.kompany.sovereign;

/**
 * 
 * @author ypai
 * 
 * @param <T>
 */
public interface DataSerializer<T> {

    public byte[] serialize(T data) throws Exception;

    public T deserialize(byte[] bytes) throws Exception;

}
