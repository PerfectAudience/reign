package org.kompany.overlord.coord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.zookeeper.data.ACL;
import org.kompany.overlord.AbstractActiveService;
import org.kompany.overlord.PathContext;
import org.kompany.overlord.PathType;
import org.kompany.overlord.Sovereign;
import org.kompany.overlord.conf.ConfObserver;
import org.kompany.overlord.conf.ConfSerializer;
import org.kompany.overlord.conf.ConfService;
import org.kompany.overlord.conf.JsonConfSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides resources for coordination like locks, semaphores, barriers, etc.
 * 
 * @author ypai
 * 
 */
public class CoordinationService extends AbstractActiveService {
    private static final Logger logger = LoggerFactory.getLogger(CoordinationService.class);

    private ZkLockManager zkLockManager;

    /** what is the maximum amount of time a lock can be held: -1 for indefinite */
    private final long maxLockHoldTimeMillis = -1;

    public CoordinationService() {
        super();

        // by default, check/clean-up locks every minute
        this.setExecutionIntervalMillis(60000);
    }

    public void observeLock(String relativeLockPath, LockObserver observer) {

    }

    public void observeSemaphore(String relativeSemaphorePath, SemaphoreObserver observer) {

    }

    /**
     * Get exclusive lock.
     * 
     * @param ownerId
     * @param relativeLockPath
     * @return
     */
    public Lock getLock(String ownerId, String relativeLockPath) {
        return getLock(ownerId, relativeLockPath, Sovereign.DEFAULT_ACL_LIST);
    }

    /**
     * Get exclusive lock.
     * 
     * @param ownerId
     * @param relativeLockPath
     * @param aclList
     * @return
     */
    public Lock getLock(String ownerId, String relativeLockPath, List<ACL> aclList) {
        return new ZkLock(zkLockManager, ownerId, PathContext.USER, relativeLockPath, ReservationType.EXCLUSIVE,
                aclList);
    }

    /**
     * 
     * @param ownerId
     * @param relativeLockPath
     * @return
     */
    public ReadWriteLock getReadWriteLock(String ownerId, String relativeLockPath) {
        return getReadWriteLock(ownerId, relativeLockPath, Sovereign.DEFAULT_ACL_LIST);
    }

    /**
     * 
     * @param ownerId
     * @param relativeLockPath
     * @param aclList
     * @return
     */
    public ReadWriteLock getReadWriteLock(String ownerId, String relativeLockPath, List<ACL> aclList) {
        return new ZkReadWriteLock(zkLockManager, ownerId, PathContext.USER, relativeLockPath, aclList);
    }

    public ZkSemaphore getSemaphore(String ownerId, String relativeSemaphorePath, int permitPoolSize) {
        return getSemaphore(ownerId, relativeSemaphorePath, permitPoolSize, Sovereign.DEFAULT_ACL_LIST, false);
    }

    /**
     * Initial creation of semaphore may result in more writes.
     * 
     * @param ownerId
     * @param relativeSemaphorePath
     * @param permitPoolSize
     * @param aclList
     * @param overwriteExistingConfig
     * @return
     */
    public synchronized ZkSemaphore getSemaphore(String ownerId, String relativeSemaphorePath, int permitPoolSize,
            List<ACL> aclList, boolean overwriteExistingConfigIfDifferent) {

        // read configuration
        ConfService confService = (ConfService) getServiceDirectory().getService("conf");
        String absoluteSemaphoreConfPath = getPathScheme().getAbsolutePath(PathContext.USER, PathType.COORD,
                ReservationType.PERMIT.getSubCategoryPathToken() + "/" + relativeSemaphorePath);
        ConfSerializer<Map<String, String>> confSerializer = new JsonConfSerializer();
        Map<String, String> semaphoreConf = confService.getConfAbsolutePath(absoluteSemaphoreConfPath, confSerializer,
                null, true);
        if (semaphoreConf != null && semaphoreConf.get("permitPoolSize") == null) {
            semaphoreConf = null;
        }

        // check if we should force overwrite because config is different
        if (overwriteExistingConfigIfDifferent) {
            if (semaphoreConf != null) {
                if (!semaphoreConf.get("permitPoolSize").equals(Integer.toString(permitPoolSize))) {
                    // set to null to force overwrite
                    semaphoreConf = null;
                }
            }
        }

        /** write out new configuration if one does not exist **/
        final Object monitorObject = this;

        if (semaphoreConf == null || semaphoreConf.get("permitPoolSize") == null) {
            logger.debug("Creating new semaphore configuration:  path={}", absoluteSemaphoreConfPath);

            // write configuration if necessary
            semaphoreConf = new HashMap<String, String>(1, 0.9f);
            semaphoreConf.put("permitPoolSize", Integer.toString(permitPoolSize));

            // set up observer to signal when write completes in ZK
            confService.getConfAbsolutePath(absoluteSemaphoreConfPath, confSerializer,
                    new ConfObserver<Map<String, String>>() {
                        @Override
                        public void updated(Map<String, String> data) {
                            synchronized (monitorObject) {
                                monitorObject.notifyAll();
                            }
                        }
                    }, true);
            confService.putConfAbsolutePath(absoluteSemaphoreConfPath, semaphoreConf, confSerializer, aclList);

            // will be unlocked by observer
            synchronized (this) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    logger.warn("Interrupted while waiting:  " + e, e);
                }
            }

            logger.info("Created new semaphore configuration:  path={}; conf={}", absoluteSemaphoreConfPath,
                    semaphoreConf);
        }

        /** read Semaphore configuration **/
        logger.debug("Reading semaphore configuration:  path={}; conf={}", absoluteSemaphoreConfPath, semaphoreConf);
        permitPoolSize = Integer.parseInt(semaphoreConf.get("permitPoolSize"));
        logger.info("Read semaphore configuration:  path={}; conf={}", absoluteSemaphoreConfPath, semaphoreConf);

        return new ZkSemaphore(zkLockManager, ownerId, PathContext.USER, relativeSemaphorePath, aclList, permitPoolSize);
    }

    public long getMaxLockHoldTimeMillis() {
        return maxLockHoldTimeMillis;
    }

    @Override
    public void perform() {
        /** check for long held locks and remove if they exceed max hold time **/

        // get exclusive lock to perform long held lock checking

        // traverse lock tree and remove any long held locks that exceed
        // threshold

    }

    @Override
    public void init() {
        zkLockManager = new ZkLockManager(getZkClient(), getPathScheme(), getPathCache());

    }

    @Override
    public void destroy() {
        zkLockManager.shutdown();

    }

}
