package org.kompany.sovereign.coord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.data.ACL;
import org.kompany.sovereign.DataSerializer;
import org.kompany.sovereign.JsonDataSerializer;
import org.kompany.sovereign.PathContext;
import org.kompany.sovereign.PathType;
import org.kompany.sovereign.conf.ConfObserver;
import org.kompany.sovereign.conf.ConfService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class ConfiguredPermitPoolSize extends ConfObserver<Map<String, String>> implements PermitPoolSize {

    private static final Logger logger = LoggerFactory.getLogger(ConfiguredPermitPoolSize.class);

    private volatile int size;

    private ConfService confService;
    private PathContext pathContext;
    private String clusterId;
    private String entityName;
    private boolean createIfNecessary;

    public ConfiguredPermitPoolSize(int permitPoolSize) {
        this.size = permitPoolSize;
    }

    public ConfiguredPermitPoolSize(ConfService confService, PathContext pathContext, String clusterId,
            String entityName, int permitPoolSize, boolean createIfNecessary) {
        if (permitPoolSize < 1) {
            throw new IllegalArgumentException("Invalid permitPoolSize:  clusterId=" + clusterId + "; entityName="
                    + entityName + "; permitPoolSize=" + size);
        }
        this.size = permitPoolSize;
        this.confService = confService;
        this.pathContext = pathContext;
        this.clusterId = clusterId;
        this.entityName = entityName;
        this.createIfNecessary = createIfNecessary;
    }

    @Override
    public ConfiguredPermitPoolSize initialize() {
        // read config
        Map<String, String> semaphoreConf = getSemaphoreConf(confService, pathContext, clusterId, entityName, this);

        /** check existing config **/
        if (semaphoreConf == null || semaphoreConf.get("permitPoolSize") == null) {
            // check to see if we should write out new configuration if one does
            // not exist
            if (createIfNecessary) {
                if (size < 1) {
                    throw new IllegalArgumentException("Invalid permitPoolSize:  clusterId=" + clusterId
                            + "; entityName=" + entityName + "; permitPoolSize=" + size);
                }
                setSemaphoreConf(confService, pathContext, clusterId, entityName, size);
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
        setSemaphoreConf(confService, PathContext.USER, clusterId, semaphoreName, permitPoolSize,
                confService.getDefaultAclList());
    }

    public static void setSemaphoreConf(ConfService confService, PathContext pathContext, String clusterId,
            String semaphoreName, int permitPoolSize) {
        setSemaphoreConf(confService, pathContext, clusterId, semaphoreName, permitPoolSize,
                confService.getDefaultAclList());
    }

    public static void setSemaphoreConf(ConfService confService, PathContext pathContext, String clusterId,
            String semaphoreName, int permitPoolSize, List<ACL> aclList) {
        // write configuration to ZK
        Map<String, String> semaphoreConf = new HashMap<String, String>(1, 0.9f);
        semaphoreConf.put("permitPoolSize", Integer.toString(permitPoolSize));

        // write out to ZK
        DataSerializer<Map<String, String>> confSerializer = new JsonDataSerializer<Map<String, String>>();
        confService.putConfAbsolutePath(pathContext, PathType.COORD, clusterId,
                confService.getPathScheme().join(ReservationType.SEMAPHORE.category(), semaphoreName), semaphoreConf,
                confSerializer, aclList);
    }

    /**
     * 
     * @param clusterId
     * @param semaphoreName
     * @return
     */
    public static Map<String, String> getSemaphoreConf(ConfService confService, String clusterId, String semaphoreName) {
        return getSemaphoreConf(confService, PathContext.USER, clusterId, semaphoreName, null);
    }

    public static Map<String, String> getSemaphoreConf(ConfService confService, PathContext pathContext,
            String clusterId, String semaphoreName, ConfObserver<Map<String, String>> confObserver) {
        // read configuration
        DataSerializer<Map<String, String>> confSerializer = new JsonDataSerializer<Map<String, String>>();
        Map<String, String> semaphoreConf = confService.getConfAbsolutePath(pathContext, PathType.COORD, clusterId,
                confService.getPathScheme().join(ReservationType.SEMAPHORE.category(), semaphoreName), confSerializer,
                confObserver, true);
        return semaphoreConf;
    }
}
