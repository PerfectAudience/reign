package org.kompany.overlord.coord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.zookeeper.data.ACL;
import org.kompany.overlord.AbstractActiveService;
import org.kompany.overlord.DataSerializer;
import org.kompany.overlord.JsonDataSerializer;
import org.kompany.overlord.PathContext;
import org.kompany.overlord.PathType;
import org.kompany.overlord.Sovereign;
import org.kompany.overlord.conf.ConfService;
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
     * Get reentrant exclusive lock.
     * 
     * @param ownerId
     * @param relativeLockPath
     * @return
     */
    public DistributedReentrantLock getReentrantLock(String ownerId, String relativeLockPath) {
        return getReentrantLock(ownerId, relativeLockPath, Sovereign.DEFAULT_ACL_LIST);
    }

    /**
     * Get reentrant exclusive lock.
     * 
     * @param ownerId
     * @param relativeLockPath
     * @param aclList
     * @return
     */
    public DistributedReentrantLock getReentrantLock(String ownerId, String relativeLockPath, List<ACL> aclList) {
        return new ZkReentrantLock(zkLockManager, ownerId, PathContext.USER, relativeLockPath,
                ReservationType.LOCK_EXCLUSIVE, aclList);
    }

    /**
     * Get exclusive lock.
     * 
     * @param ownerId
     * @param relativeLockPath
     * @return
     */
    public DistributedLock getLock(String ownerId, String relativeLockPath) {
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
    public DistributedLock getLock(String ownerId, String relativeLockPath, List<ACL> aclList) {
        return new ZkLock(zkLockManager, ownerId, PathContext.USER, relativeLockPath, ReservationType.LOCK_EXCLUSIVE,
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

    /**
     * 
     * @param ownerId
     * @param relativeSemaphorePath
     * @param permitPoolSize
     * @return
     */
    public DistributedSemaphore getFixedSemaphore(String ownerId, String relativeSemaphorePath, int permitPoolSize) {
        return getSemaphore(ownerId, relativeSemaphorePath, new ConstantPermitPoolSize(permitPoolSize),
                Sovereign.DEFAULT_ACL_LIST);
    }

    public DistributedSemaphore getFixedSemaphore(String ownerId, String relativeSemaphorePath, int permitPoolSize,
            List<ACL> aclList) {
        return getSemaphore(ownerId, relativeSemaphorePath, new ConstantPermitPoolSize(permitPoolSize), aclList);
    }

    /**
     * 
     * @param ownerId
     * @param relativeSemaphorePath
     * @param desiredPermitPoolSize
     * @param createConfigurationIfNecessary
     * @return
     */
    public DistributedSemaphore getConfiguredSemaphore(String ownerId, String relativeSemaphorePath,
            int desiredPermitPoolSize, boolean createConfigurationIfNecessary) {
        return getSemaphore(ownerId, relativeSemaphorePath, new ZkConfiguredPermitPoolSize(getPathScheme(),
                (ConfService) getServiceDirectory().getService("conf"), relativeSemaphorePath, desiredPermitPoolSize,
                Sovereign.DEFAULT_ACL_LIST, createConfigurationIfNecessary), Sovereign.DEFAULT_ACL_LIST);
    }

    public DistributedSemaphore getConfiguredSemaphore(String ownerId, String relativeSemaphorePath,
            int permitPoolSize, boolean createConfigurationIfNecessary, List<ACL> aclList) {
        return getSemaphore(ownerId, relativeSemaphorePath, new ZkConfiguredPermitPoolSize(getPathScheme(),
                (ConfService) getServiceDirectory().getService("conf"), relativeSemaphorePath, permitPoolSize, aclList,
                createConfigurationIfNecessary), aclList);
    }

    public void setSemaphoreConf(String relativeSemaphorePath, int permitPoolSize) {
        setSemaphoreConf(relativeSemaphorePath, permitPoolSize, Sovereign.DEFAULT_ACL_LIST);
    }

    public void setSemaphoreConf(String relativeSemaphorePath, int permitPoolSize, List<ACL> aclList) {
        // write configuration to ZK
        Map<String, String> semaphoreConf = new HashMap<String, String>(1, 0.9f);
        semaphoreConf.put("permitPoolSize", Integer.toString(permitPoolSize));

        String absoluteSemaphoreConfPath = CoordServicePathUtil.getAbsolutePathEntity(getPathScheme(),
                PathContext.USER, PathType.COORD, ReservationType.SEMAPHORE, relativeSemaphorePath);
        ConfService confService = (ConfService) getServiceDirectory().getService("conf");
        DataSerializer<Map<String, String>> confSerializer = new JsonDataSerializer<Map<String, String>>();
        confService.putConfAbsolutePath(absoluteSemaphoreConfPath, semaphoreConf, confSerializer, aclList);
    }

    /**
     * @param ownerId
     * @param relativeSemaphorePath
     * @param permitPoolSizeFunction
     * @param aclList
     * @return
     */
    public DistributedSemaphore getSemaphore(String ownerId, String relativeSemaphorePath,
            PermitPoolSize permitPoolSizeFunction, List<ACL> aclList) {
        return new ZkSemaphore(zkLockManager, ownerId, PathContext.USER, relativeSemaphorePath, aclList,
                permitPoolSizeFunction);
    }

    // /**
    // * Initial creation of semaphore may result in more writes.
    // *
    // * @param ownerId
    // * @param relativeSemaphorePath
    // * @param permitPoolSize
    // * @param aclList
    // * @param overwriteExistingConfig
    // * @return
    // */
    // public synchronized DistributedSemaphore
    // getSemaphoreSharedConfiguration(String ownerId,
    // String relativeSemaphorePath, int permitPoolSize, List<ACL> aclList,
    // boolean overwriteExistingConfigIfDifferent) {
    //
    // // read configuration
    // ConfService confService = (ConfService)
    // getServiceDirectory().getService("conf");
    // String absoluteSemaphoreConfPath =
    // getPathScheme().getAbsolutePath(PathContext.USER, PathType.COORD,
    // ReservationType.SEMAPHORE.category() + "/" + relativeSemaphorePath);
    // DataSerializer<Map<String, String>> confSerializer = new
    // JsonDataSerializer<Map<String, String>>();
    // Map<String, String> semaphoreConf =
    // confService.getConfAbsolutePath(absoluteSemaphoreConfPath,
    // confSerializer,
    // null, true);
    // if (semaphoreConf != null && semaphoreConf.get("permitPoolSize") == null)
    // {
    // semaphoreConf = null;
    // }
    //
    // // check if we should force overwrite because config is different
    // if (overwriteExistingConfigIfDifferent) {
    // if (semaphoreConf != null) {
    // if
    // (!semaphoreConf.get("permitPoolSize").equals(Integer.toString(permitPoolSize)))
    // {
    // // set to null to force overwrite
    // semaphoreConf = null;
    // }
    // }
    // }
    //
    // /** write out new configuration if one does not exist **/
    // final Object monitorObject = this;
    //
    // if (semaphoreConf == null || semaphoreConf.get("permitPoolSize") == null)
    // {
    // logger.debug("Creating new semaphore configuration:  path={}",
    // absoluteSemaphoreConfPath);
    //
    // // write configuration if necessary
    // semaphoreConf = new HashMap<String, String>(1, 0.9f);
    // semaphoreConf.put("permitPoolSize", Integer.toString(permitPoolSize));
    //
    // // set up observer to signal when write completes in ZK
    // confService.getConfAbsolutePath(absoluteSemaphoreConfPath,
    // confSerializer,
    // new ConfObserver<Map<String, String>>() {
    // @Override
    // public void updated(Map<String, String> data) {
    // synchronized (monitorObject) {
    // monitorObject.notifyAll();
    // }
    // }
    // }, true);
    // confService.putConfAbsolutePath(absoluteSemaphoreConfPath, semaphoreConf,
    // confSerializer, aclList);
    //
    // // will be unlocked by observer
    // synchronized (this) {
    // try {
    // wait();
    // } catch (InterruptedException e) {
    // logger.warn("Interrupted while waiting:  " + e, e);
    // }
    // }
    //
    // logger.info("Created new semaphore configuration:  path={}; conf={}",
    // absoluteSemaphoreConfPath,
    // semaphoreConf);
    // }
    //
    // /** read Semaphore configuration **/
    // logger.debug("Reading semaphore configuration:  path={}; conf={}",
    // absoluteSemaphoreConfPath, semaphoreConf);
    // permitPoolSize = Integer.parseInt(semaphoreConf.get("permitPoolSize"));
    // logger.info("Read semaphore configuration:  path={}; conf={}",
    // absoluteSemaphoreConfPath, semaphoreConf);
    //
    // final int finalPermitPoolSize = permitPoolSize;
    // return new ZkSemaphore(zkLockManager, ownerId, PathContext.USER,
    // relativeSemaphorePath, aclList,
    // new PermitPoolSize() {
    // @Override
    // public int get() {
    // return finalPermitPoolSize;
    // }
    // });
    // }

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
