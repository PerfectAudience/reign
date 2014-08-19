package io.reign;

import java.util.Map;

/**
 * 
 * @author ypai
 *
 */
public interface ServiceNodeInfo extends NodeInfo {
	public Object getAttribute(String key);

	public Map<String, String> getAttributeMap();

	public String getClusterId();

	public String getServiceId();

}
