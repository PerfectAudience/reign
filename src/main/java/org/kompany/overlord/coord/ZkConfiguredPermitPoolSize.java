package org.kompany.overlord.coord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.data.ACL;
import org.kompany.overlord.DataSerializer;
import org.kompany.overlord.JsonDataSerializer;
import org.kompany.overlord.PathContext;
import org.kompany.overlord.PathScheme;
import org.kompany.overlord.PathType;
import org.kompany.overlord.conf.ConfService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class ZkConfiguredPermitPoolSize implements PermitPoolSize {

    private static final Logger logger = LoggerFactory.getLogger(ZkConfiguredPermitPoolSize.class);

    private volatile int size;

    public ZkConfiguredPermitPoolSize(PathScheme pathScheme, ConfService confService, String entityPath,
            int desiredInitialSize, List<ACL> aclList, boolean createIfNecessary) {

        /** read configuration **/
        String absoluteSemaphoreConfPath = CoordServicePathUtil.getAbsolutePathEntity(pathScheme, PathContext.USER,
                PathType.COORD, ReservationType.SEMAPHORE, entityPath);
        DataSerializer<Map<String, String>> confSerializer = new JsonDataSerializer<Map<String, String>>();
        Map<String, String> semaphoreConf = confService.getConfAbsolutePath(absoluteSemaphoreConfPath, confSerializer,
                null, true);

        /** write out new configuration if one does not exist **/
        if (semaphoreConf == null || semaphoreConf.get("permitPoolSize") == null) {
            if (createIfNecessary) {
                logger.debug("Creating new semaphore configuration:  path={}", absoluteSemaphoreConfPath);

                // write configuration if necessary
                semaphoreConf = new HashMap<String, String>(1, 0.9f);
                semaphoreConf.put("permitPoolSize", Integer.toString(desiredInitialSize));

                // put config in ZK
                confService.putConfAbsolutePath(absoluteSemaphoreConfPath, semaphoreConf, confSerializer, aclList);

                logger.info("Created new semaphore configuration:  path={}; conf={}", absoluteSemaphoreConfPath,
                        semaphoreConf);

                this.size = desiredInitialSize;
            } else {
                throw new IllegalStateException("Semaphore configuration does not exist:  path=" + entityPath);
            }
        } else {
            this.size = Integer.parseInt(semaphoreConf.get("permitPoolSize"));
        }
    }

    @Override
    public int get() {
        return size;
    }

}
