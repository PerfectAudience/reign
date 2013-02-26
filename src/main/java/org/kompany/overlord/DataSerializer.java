package org.kompany.overlord;

public interface DataSerializer<T> {

    public byte[] serialize(T data) throws Exception;

    public T deserialize(byte[] bytes) throws Exception;

}
