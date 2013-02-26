package org.kompany.overlord.presence;

import java.util.Map;

import org.kompany.overlord.DataSerializer;

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
