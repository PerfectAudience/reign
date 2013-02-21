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

    public ZkSemaphore getSemaphore(String ownerId, String relativeSemaphorePath, int maxAvailablePermits) {
        return getSemaphore(ownerId, relativeSemaphorePath, maxAvailablePermits, Sovereign.DEFAULT_ACL_LIST);
    }

    public ZkSemaphore getSemaphore(String ownerId, String relativeSemaphorePath, int maxAvailablePermits,
            List<ACL> aclList) {
        // attempt to get lock to create Semaphore configuration

        // read configuration
        ConfService confService = (ConfService) getServiceDirectory().getService("conf");
        String absoluteSemaphoreConfPath = getPathScheme().getAbsolutePath(PathContext.USER, PathType.COORD,
                ReservationType.PERMIT.getSubCategoryPathToken() + "/" + relativeSemaphorePath);
        ConfSerializer<Map<String, String>> confSerializer = new JsonConfSerializer();
        Map<String, String> semaphoreConf = confService.getConfAbsolutePath(absoluteSemaphoreConfPath, confSerializer,
                null, true);

        // write out new configuration if one does not exist
        while (semaphoreConf == null || semaphoreConf.size() < 1 || semaphoreConf.get("maxAvailablePermits") == null) {
            logger.info("Creating new semaphore configuration:  path={}", absoluteSemaphoreConfPath);
            Lock lock = getLock(ownerId, relativeSemaphorePath, aclList);
            if (lock.tryLock()) {
                try {
                    // write configuration if necessary
                    semaphoreConf = new HashMap<String, String>(1, 0.9f);
                    semaphoreConf.put("maxAvailablePermits", Integer.toString(maxAvailablePermits));
                    confService.putConfAbsolutePath(absoluteSemaphoreConfPath, semaphoreConf, confSerializer, aclList);

                } finally {
                    lock.unlock();
                }
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.warn("Interrupted while waiting for semaphore config:  " + e + ":  path="
                        + absoluteSemaphoreConfPath, e);
            }

            semaphoreConf = confService.getConfAbsolutePath(absoluteSemaphoreConfPath, confSerializer, null, true);
        }

        // read Semaphore configuration
        logger.info("Reading existing semaphore configuration:  path={}; conf={}", absoluteSemaphoreConfPath,
                semaphoreConf);
        maxAvailablePermits = Integer.parseInt(semaphoreConf.get("maxAvailablePermits"));

        return new ZkSemaphore(zkLockManager, ownerId, PathContext.USER, relativeSemaphorePath, aclList,
                maxAvailablePermits);
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
