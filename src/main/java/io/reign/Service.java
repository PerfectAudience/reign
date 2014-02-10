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
import io.reign.zk.PathCache;

import java.util.List;

import org.apache.zookeeper.data.ACL;

/**
 * Common interface for framework service plug-ins.<br/>
 * <br/>
 * Life cycle for a service:<br/>
 * <ol>
 * <li> {@link setPathScheme()}
 * <li> {@link setZkClient()}
 * <li> {@link init()}
 * <li> {@link destroy()}
 * </ol>
 * 
 * @author ypai
 * 
 */
public interface Service {

    public ReignContext getContext();

    public void setContext(ReignContext serviceDirectory);

    // public void setPathCache(PathCache pathCache);

    public void setDefaultZkAclList(List<ACL> defaultZkAclList);

    /**
     * 
     * @param pathScheme
     */
    public void setPathScheme(PathScheme pathScheme);

    /**
     * 
     * @param zkClient
     */
    public void setZkClient(ZkClient zkClient);

    /**
     * 
     * @param message
     */
    public ResponseMessage handleMessage(RequestMessage message);

    /**
     * Initialize the service.
     */
    public void init();

    /**
     * Clean up resources as necessary.
     */
    public void destroy();

}
