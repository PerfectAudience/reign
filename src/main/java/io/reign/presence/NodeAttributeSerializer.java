package io.reign.presence;

import io.reign.DataSerializer;

import java.util.Map;


/**
 * Used to serialize NodeInfo. Should be re-usable.
 * 
 * @author ypai
 * 
 */
public interface NodeAttributeSerializer extends DataSerializer<Map<String, String>> {

    public byte[] serialize(Map<String, String> map) throws Exception;

    public Map<String, String> deserialize(byte[] bytes) throws Exception;
}
