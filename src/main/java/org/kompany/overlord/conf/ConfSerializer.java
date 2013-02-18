package org.kompany.overlord.conf;

public interface ConfSerializer<T> {

    public byte[] serialize(T conf) throws Exception;

    public T deserialize(byte[] bytes) throws Exception;

}
