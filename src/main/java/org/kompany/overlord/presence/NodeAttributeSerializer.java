package org.kompany.overlord.presence;

import java.util.Map;

/**
 * Used to serialize NodeInfo. Should be re-usable.
 * 
 * @author ypai
 * 
 */
public interface NodeAttributeSerializer {

    public byte[] serialize(Map<String, String> map) throws Exception;

    public Map<String, Object> deserialize(byte[] bytes) throws Exception;
}
