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

package io.reign;

import io.reign.zk.PathCache;

import java.util.List;

import org.apache.zookeeper.data.ACL;

/**
 * Provides access to essential framework capabilities without exposing base framework object.
 * 
 * @author ypai
 * 
 */
public interface ReignContext {

    public <T extends Service> T getService(String serviceName);

    /**
     * 
     * @return NEW instance of CanonicalId
     */
    public CanonicalId getCanonicalId();

    public String getCanonicalIdPathToken();

    public ZkClient getZkClient();

    public PathScheme getPathScheme();

    public List<ACL> getDefaultZkAclList();

    // public PathCache getPathCache();
}
