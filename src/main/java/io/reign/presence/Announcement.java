/*
 Copyright 2013 Yen Pai ypai@reign.io

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

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
