package org.kompany.overlord.coord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.ACL;
import org.kompany.overlord.AbstractActiveService;
import org.kompany.overlord.DataSerializer;
import org.kompany.overlord.JsonDataSerializer;
import org.kompany.overlord.ObservableService;
import org.kompany.overlord.PathContext;
import org.kompany.overlord.PathType;
import org.kompany.overlord.ServiceObserver;
import org.kompany.overlord.ServiceObserverManager;
import org.kompany.overlord.ServiceObserverWrapper;
import org.kompany.overlord.Sovereign;
import org.kompany.overlord.conf.ConfObserver;
import org.kompany.overlord.conf.ConfService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides resources for coordination like locks, semaphores, barriers, etc.
 * 
 * @author ypai
 * 
 */
public class CoordinationService extends AbstractActiveService implements ObservableService {
    private static final Logger logger = LoggerFactory.getLogger(CoordinationService.class);

    private ZkReservationManager zkReservationManager;

    /** what is the maximum amount of time a lock can be held: -1 for indefinite */
    private final long maxLockHoldTimeMillis = -1;

    private final ServiceObserverManager<CoordObserverWrapper> observerManager = new ServiceObserverManager<CoordObserverWrapper>();

    public CoordinationService() {
        super();

        // by default, check/clean-up locks every minute
        this.setExecutionIntervalMillis(60000);
    }

    public void observeLock(String clusterId, String relativeLockPath, LockObserver observer) {

    }

    public void observeSemaphore(String clusterId, String relativeSemaphorePath, SemaphoreObserver observer) {

    }

    /**
     * Get reentrant exclusive lock.
     * 
     * @param ownerId
     * @param relativeLockPath
     * @return
     */
    public DistributedReentrantLock getReentrantLock(String ownerId, String clusterId, String lockName) {
        return getReentrantLock(ownerId, clusterId, lockName, Sovereign.DEFAULT_ACL_LIST);
    }

    /**
     * Get reentrant exclusive lock.
     * 
     * @param ownerId
     * @param relativeLockPath
     * @param aclList
     * @return
     */
    public DistributedReentrantLock getReentrantLock(String ownerId, String clusterId, String lockName,
            List<ACL> aclList) {
        return new ZkReentrantLock(zkReservationManager, ownerId, PathContext.USER, clusterId, lockName,
                ReservationType.LOCK_EXCLUSIVE, aclList);
    }

    /**
     * Get exclusive lock.
     * 
     * @param ownerId
     * @param relativeLockPath
     * @return
     */
    public DistributedLock getLock(String ownerId, String clusterId, String lockName) {
        return getLock(ownerId, clusterId, lockName, Sovereign.DEFAULT_ACL_LIST);
    }

    /**
     * Get exclusive lock.
     * 
     * @param ownerId
     * @param relativeLockPath
     * @param aclList
     * @return
     */
    public DistributedLock getLock(String ownerId, String clusterId, String lockName, List<ACL> aclList) {
        return new ZkLock(zkReservationManager, ownerId, PathContext.USER, clusterId, lockName,
                ReservationType.LOCK_EXCLUSIVE, aclList);
    }

    /**
     * 
     * @param ownerId
     * @param relativeLockPath
     * @return
     */
    public ReadWriteLock getReadWriteLock(String ownerId, String clusterId, String lockName) {
        return getReadWriteLock(ownerId, clusterId, lockName, Sovereign.DEFAULT_ACL_LIST);
    }

    /**
     * 
     * @param ownerId
     * @param relativeLockPath
     * @param aclList
     * @return
     */
    public ReadWriteLock getReadWriteLock(String ownerId, String clusterId, String lockName, List<ACL> aclList) {
        return new ZkReadWriteLock(zkReservationManager, ownerId, PathContext.USER, clusterId, lockName, aclList);
    }

    /**
     * 
     * @param ownerId
     * @param relativeSemaphorePath
     * @param permitPoolSize
     * @return
     */
    public DistributedSemaphore getFixedSemaphore(String ownerId, String clusterId, String semaphoreName,
            int permitPoolSize) {
        return getSemaphore(ownerId, clusterId, semaphoreName, new ConstantPermitPoolSize(permitPoolSize),
                Sovereign.DEFAULT_ACL_LIST);
    }

    public DistributedSemaphore getFixedSemaphore(String ownerId, String clusterId, String semaphoreName,
            int permitPoolSize, List<ACL> aclList) {
        return getSemaphore(ownerId, clusterId, semaphoreName, new ConstantPermitPoolSize(permitPoolSize), aclList);
    }

    /**
     * 
     * @param ownerId
     * @param clusterId
     * @param semaphoreName
     * @return
     */
    public DistributedSemaphore getConfiguredSemaphore(String ownerId, String clusterId, String semaphoreName) {
        return getConfiguredSemaphore(ownerId, clusterId, semaphoreName, -1, false, Sovereign.DEFAULT_ACL_LIST);
    }

    public DistributedSemaphore getConfiguredSemaphore(String ownerId, String clusterId, String semaphoreName,
            List<ACL> aclList) {
        return getConfiguredSemaphore(ownerId, clusterId, semaphoreName, -1, false, aclList);
    }

    /**
     * 
     * @param ownerId
     * @param relativeSemaphorePath
     * @param desiredPermitPoolSize
     * @param createConfigurationIfNecessary
     * @return
     */
    public DistributedSemaphore getConfiguredSemaphore(String ownerId, String clusterId, String semaphoreName,
            int permitPoolSize, boolean createConfigurationIfNecessary) {
        return getConfiguredSemaphore(ownerId, clusterId, semaphoreName, permitPoolSize,
                createConfigurationIfNecessary, Sovereign.DEFAULT_ACL_LIST);
    }

    public DistributedSemaphore getConfiguredSemaphore(String ownerId, String clusterId, String semaphoreName,
            int permitPoolSize, boolean createConfigurationIfNecessary, List<ACL> aclList) {
        /**
         * create permit pool size function and add observer to adjust as
         * necessary
         **/
        final VariablePermitPoolSize pps = new VariablePermitPoolSize(permitPoolSize);
        Map<String, String> semaphoreConf = getSemaphoreConf(clusterId, semaphoreName,
                new ConfObserver<Map<String, String>>() {
                    @Override
                    public void updated(Map<String, String> data) {
                        int permitPoolSize = Integer.parseInt(data.get("permitPoolSize"));
                        pps.set(permitPoolSize);
                    }
                });

        /** check existing config **/
        if (semaphoreConf == null || semaphoreConf.get("permitPoolSize") == null) {
            // check tosee if we should write out new configuration if one does
            // not exist
            if (createConfigurationIfNecessary) {
                if (permitPoolSize < 1) {
                    throw new IllegalArgumentException("Invalid permitPoolSize:  clusterId=" + clusterId
                            + "; semaphoreName=" + semaphoreName + "; permitPoolSize=" + permitPoolSize);
                }
                setSemaphoreConf(clusterId, semaphoreName, permitPoolSize);
            } else {
                throw new IllegalStateException("Semaphore configuration does not exist:  clusterId=" + clusterId
                        + "; semaphoreName=" + semaphoreName);
            }
        } else {
            // read to make sure value is valid
            permitPoolSize = Integer.parseInt(semaphoreConf.get("permitPoolSize"));
            logger.info("Found semaphore configuration:  clusterId={}; semaphoreName={}; permitPoolSize={}",
                    new Object[] { clusterId, semaphoreName, permitPoolSize });
        }

        return getSemaphore(ownerId, clusterId, semaphoreName, pps, aclList);
    }

    public void setSemaphoreConf(String clusterId, String semaphoreName, int permitPoolSize) {
        setSemaphoreConf(clusterId, semaphoreName, permitPoolSize, Sovereign.DEFAULT_ACL_LIST);
    }

    public void setSemaphoreConf(String clusterId, String semaphoreName, int permitPoolSize, List<ACL> aclList) {
        // write configuration to ZK
        Map<String, String> semaphoreConf = new HashMap<String, String>(1, 0.9f);
        semaphoreConf.put("permitPoolSize", Integer.toString(permitPoolSize));

        // write out to ZK
        ConfService confService = (ConfService) getServiceDirectory().getService("conf");
        DataSerializer<Map<String, String>> confSerializer = new JsonDataSerializer<Map<String, String>>();
        confService.putConfAbsolutePath(PathContext.USER, PathType.COORD, clusterId,
                getPathScheme().join(ReservationType.SEMAPHORE.category(), semaphoreName), semaphoreConf,
                confSerializer, aclList);
    }

    public Map<String, String> getSemaphoreConf(String clusterId, String semaphoreName) {
        return getSemaphoreConf(clusterId, semaphoreName, null);
    }

    Map<String, String> getSemaphoreConf(String clusterId, String semaphoreName,
            ConfObserver<Map<String, String>> confObserver) {
        // read configuration
        ConfService confService = (ConfService) getServiceDirectory().getService("conf");
        DataSerializer<Map<String, String>> confSerializer = new JsonDataSerializer<Map<String, String>>();
        Map<String, String> semaphoreConf = confService.getConfAbsolutePath(PathContext.USER, PathType.COORD,
                clusterId, getPathScheme().join(ReservationType.SEMAPHORE.category(), semaphoreName), confSerializer,
                confObserver, true);
        return semaphoreConf;
    }

    /**
     * @param ownerId
     * @param relativeSemaphorePath
     * @param permitPoolSizeFunction
     * @param aclList
     * @return
     */
    public DistributedSemaphore getSemaphore(String ownerId, String clusterId, String semaphoreName,
            PermitPoolSize permitPoolSizeFunction, List<ACL> aclList) {
        return new ZkSemaphore(zkReservationManager, ownerId, PathContext.USER, clusterId, semaphoreName, aclList,
                permitPoolSizeFunction);
    }

    public long getMaxLockHoldTimeMillis() {
        return maxLockHoldTimeMillis;
    }

    @Override
    public void perform() {
        /** get exclusive leader lock to perform maintenance duties **/

        /** semaphore maintenance **/
        // list semaphores

        // acquire lock on a semaphore

        // revoke any permits that have exceeded the limit

        /** lock maintenance **/
        // list locks

        // get exclusive lock on a given lock to perform long held lock checking

        // traverse lock tree and remove any long held locks that exceed
        // threshold

        /** barrier maintenance **/
    }

    @Override
    public void init() {
        zkReservationManager = new ZkReservationManager(getZkClient(), getPathScheme(), getPathCache());

    }

    @Override
    public void destroy() {
        zkReservationManager.shutdown();

    }

    @Override
    public void signalStateReset(Object o) {
        // TODO Auto-generated method stub

    }

    @Override
    public void signalStateUnknown(Object o) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean filterWatchedEvent(WatchedEvent event) {
        return !observerManager.isBeingObserved(event.getPath());

    }

    @Override
    public void nodeDataChanged(WatchedEvent event) {
        String path = event.getPath();
    }

    @Override
    public void nodeChildrenChanged(WatchedEvent event) {
        observerManager.signal(event.getPath(), null);
    }

    private static abstract class CoordObserverWrapper<T extends ServiceObserver> extends ServiceObserverWrapper<T> {

    }

    private static class LockObserverWrapper extends CoordObserverWrapper<LockObserver> {
        @Override
        public void signalObserver(Object o) {
            // TODO Auto-generated method stub

        }

    }

    private static class SemaphoreObserverWrapper extends CoordObserverWrapper<SemaphoreObserver> {
        @Override
        public void signalObserver(Object o) {
            // TODO Auto-generated method stub

        }

    }
}
