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

package io.reign.coord;

import io.reign.DataSerializer;
import io.reign.JsonDataSerializer;
import io.reign.conf.ConfService;
import io.reign.conf.SimpleConfObserver;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class ConfiguredPermitPoolSize extends SimpleConfObserver<Map<String, String>> implements PermitPoolSize {

    private static final Logger logger = LoggerFactory.getLogger(ConfiguredPermitPoolSize.class);

    private volatile int size;

    private ConfService confService;

    private String clusterId;
    private String entityName;
    private boolean createIfNecessary;

    public ConfiguredPermitPoolSize(int permitPoolSize) {
        this.size = permitPoolSize;
    }

    public ConfiguredPermitPoolSize(ConfService confService, String clusterId, String entityName, int permitPoolSize,
            boolean createIfNecessary) {
        if (permitPoolSize < 1) {
            throw new IllegalArgumentException("Invalid permitPoolSize:  clusterId=" + clusterId + "; entityName="
                    + entityName + "; permitPoolSize=" + size);
        }
        this.size = permitPoolSize;
        this.confService = confService;

        this.clusterId = clusterId;
        this.entityName = entityName;
        this.createIfNecessary = createIfNecessary;
    }

    @Override
    public ConfiguredPermitPoolSize initialize() {
        // read config
        Map<String, String> semaphoreConf = getSemaphoreConf(confService, clusterId, entityName, this);

        /** check existing config **/
        if (semaphoreConf == null || semaphoreConf.get("permitPoolSize") == null) {
            // check to see if we should write out new configuration if one does
            // not exist
            if (createIfNecessary) {
                if (size < 1) {
                    throw new IllegalArgumentException("Invalid permitPoolSize:  clusterId=" + clusterId
                            + "; entityName=" + entityName + "; permitPoolSize=" + size);
                }
                setSemaphoreConf(confService, clusterId, entityName, size);
            } else {
                throw new IllegalStateException("Semaphore configuration does not exist:  clusterId=" + clusterId
                        + "; semaphoreName=" + entityName);
            }
        } else {
            // read to make sure value is valid
            size = Integer.parseInt(semaphoreConf.get("permitPoolSize"));
            logger.debug("Found semaphore configuration:  clusterId={}; entityName={}; permitPoolSize={}",
                    new Object[] { clusterId, entityName, size });
        }

        return this;
    }

    @Override
    public int get() {
        return size;
    }

    @Override
    public void updated(Map<String, String> data) {
        this.size = Integer.parseInt(data.get("permitPoolSize"));
        logger.info("Permit pool size updated:  size={}", size);
    }

    /**
     * 
     * @param clusterId
     * @param semaphoreName
     * @param permitPoolSize
     */
    public static void setSemaphoreConf(ConfService confService, String clusterId, String semaphoreName,
            int permitPoolSize) {
        setSemaphoreConf(confService, clusterId, semaphoreName, permitPoolSize, confService.getDefaultAclList());
    }

    public static void setSemaphoreConf(ConfService confService, String clusterId, String semaphoreName,
            int permitPoolSize, List<ACL> aclList) {
        // write configuration to ZK
        Map<String, String> semaphoreConf = new HashMap<String, String>(1, 0.9f);
        semaphoreConf.put("permitPoolSize", Integer.toString(permitPoolSize));

        // write out to ZK
        DataSerializer<Map<String, String>> confSerializer = new JsonDataSerializer<Map<String, String>>();
        confService.putConf(clusterId,
                confService.getPathScheme().joinPaths(ReservationType.SEMAPHORE.category(), semaphoreName), semaphoreConf,
                confSerializer, aclList);
    }

    /**
     * 
     * @param clusterId
     * @param semaphoreName
     * @return
     */
    public static Map<String, String> getSemaphoreConf(ConfService confService, String clusterId, String semaphoreName) {
        return getSemaphoreConf(confService, clusterId, semaphoreName, null);
    }

    public static Map<String, String> getSemaphoreConf(ConfService confService, String clusterId, String semaphoreName,
            SimpleConfObserver<Map<String, String>> confObserver) {
        // read configuration
        DataSerializer<Map<String, String>> confSerializer = new JsonDataSerializer<Map<String, String>>();
        Map<String, String> semaphoreConf = confService.getConf(clusterId,
                confService.getPathScheme().joinPaths(ReservationType.SEMAPHORE.category(), semaphoreName), confSerializer,
                confObserver);
        return semaphoreConf;
    }
}
