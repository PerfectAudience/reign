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

import io.reign.mesg.RequestMessage;
import io.reign.mesg.ResponseMessage;
import io.reign.util.PathCache;

import java.util.List;

import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public abstract class AbstractService extends AbstractZkEventHandler implements Service {

    private static final Logger logger = LoggerFactory.getLogger(AbstractService.class);

    private PathScheme pathScheme;
    private ZkClient zkClient;

    private PathCache pathCache;

    private ReignContext context;

    private List<ACL> defaultAclList;

    @Override
    public ResponseMessage handleMessage(RequestMessage message) {
        return null;
    }

    public List<ACL> getDefaultAclList() {
        return defaultAclList;
    }

    @Override
    public void setDefaultAclList(List<ACL> defaultAclList) {
        this.defaultAclList = defaultAclList;
    }

    @Override
    public ReignContext getContext() {
        return context;
    }

    @Override
    public void setContext(ReignContext serviceDirectory) {
        this.context = serviceDirectory;
    }

    public PathCache getPathCache() {
        return pathCache;
    }

    @Override
    public void setPathCache(PathCache pathCache) {
        this.pathCache = pathCache;
    }

    public PathScheme getPathScheme() {
        return pathScheme;
    }

    @Override
    public void setPathScheme(PathScheme pathScheme) {
        this.pathScheme = pathScheme;

    }

    public ZkClient getZkClient() {
        return zkClient;
    }

    @Override
    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    @Override
    public abstract void init();

    @Override
    public abstract void destroy();

}
