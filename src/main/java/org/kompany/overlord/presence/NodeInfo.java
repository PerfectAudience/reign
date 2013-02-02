package org.kompany.overlord.presence;

import java.util.Map;

/**
 * 
 * @author ypai
 * 
 */
public class NodeInfo {

    private String id;

    private Map<String, Object> attributeMap;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Object getAttribute() {
        return attributeMap.get(id);
    }

    public void setAttribute(String key, String value) {
        attributeMap.put(key, value);
    }

}
