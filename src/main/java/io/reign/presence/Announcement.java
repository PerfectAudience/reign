package io.reign.presence;

import io.reign.DataSerializer;

import java.util.List;
import java.util.Map;

import org.apache.zookeeper.data.ACL;

/**
 * 
 * @author ypai
 * 
 */
public class Announcement {

    private volatile boolean hidden = true;
    private volatile NodeInfo nodeInfo;
    private volatile List<ACL> aclList;
    private volatile DataSerializer<Map<String, String>> nodeAttributeSerializer;
    private volatile long lastUpdated;

    public NodeInfo getNodeInfo() {
        return nodeInfo;
    }

    public void setNodeInfo(NodeInfo nodeInfo) {
        this.nodeInfo = nodeInfo;
    }

    public boolean isHidden() {
        return hidden;
    }

    public void setHidden(boolean hidden) {
        this.hidden = hidden;
    }

    public List<ACL> getAclList() {
        return aclList;
    }

    public void setAclList(List<ACL> aclList) {
        this.aclList = aclList;
    }

    public DataSerializer<Map<String, String>> getNodeAttributeSerializer() {
        return nodeAttributeSerializer;
    }

    public void setNodeAttributeSerializer(DataSerializer<Map<String, String>> nodeAttributeSerializer) {
        this.nodeAttributeSerializer = nodeAttributeSerializer;
    }

    public long getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(long lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

}
