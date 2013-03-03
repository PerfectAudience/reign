package org.kompany.overlord.coord;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.ACL;
import org.kompany.overlord.AbstractActiveService;
import org.kompany.overlord.DataSerializer;
import org.kompany.overlord.JsonDataSerializer;
import org.kompany.overlord.ObservableService;
import org.kompany.overlord.PathContext;
import org.kompany.overlord.PathScheme;
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

    private final CoordinationServiceCache coordinationServiceCache;

    public CoordinationService() {
        super();

        coordinationServiceCache = new CoordinationServiceCache();

        // by default, check/clean-up locks every minute
        this.setExecutionIntervalMillis(60000);
    }

    public void observeLock(String clusterId, String lockName, LockObserver observer) {
        observeLock(PathContext.USER, clusterId, lockName, observer);
    }

    public void observeSemaphore(String clusterId, String semaphoreName, SemaphoreObserver observer) {
        observeSemaphore(PathContext.USER, clusterId, semaphoreName, observer);
    }

    public void observeLock(PathContext pathContext, String clusterId, String lockName, LockObserver observer) {
        String entityPath = CoordServicePathUtil.getAbsolutePathEntity(getPathScheme(), pathContext, PathType.COORD,
                clusterId, ReservationType.LOCK_EXCLUSIVE, lockName);
        observerManager.put(entityPath, new LockObserverWrapper(entityPath, observer, coordinationServiceCache));
    }

    public void observeSemaphore(PathContext pathContext, String clusterId, String semaphoreName,
            SemaphoreObserver observer) {
        String entityPath = CoordServicePathUtil.getAbsolutePathEntity(getPathScheme(), pathContext, PathType.COORD,
                clusterId, ReservationType.SEMAPHORE, semaphoreName);
        observerManager.put(entityPath, new SemaphoreObserverWrapper(entityPath, observer, coordinationServiceCache,
                getPathScheme()));
    }

    /**
     * Get reentrant exclusive lock.
     * 
     * @param ownerId
     * @param relativeLockPath
     * @return
     */
    public DistributedReentrantLock getReentrantLock(String ownerId, String clusterId, String lockName) {
        return getReentrantLock(PathContext.USER, ownerId, clusterId, lockName, Sovereign.DEFAULT_ACL_LIST);
    }

    /**
     * Get reentrant exclusive lock.
     * 
     * @param ownerId
     * @param relativeLockPath
     * @param aclList
     * @return
     */
    public DistributedReentrantLock getReentrantLock(PathContext pathContext, String ownerId, String clusterId,
            String lockName, List<ACL> aclList) {
        String entityPath = CoordServicePathUtil.getAbsolutePathEntity(getPathScheme(), pathContext, PathType.COORD,
                clusterId, ReservationType.LOCK_EXCLUSIVE, lockName);
        DistributedLock lock = new ZkReentrantLock(zkReservationManager, ownerId, entityPath,
                ReservationType.LOCK_EXCLUSIVE, aclList);
        this.coordinationServiceCache.putLock(entityPath, ReservationType.LOCK_EXCLUSIVE, lock);

        return (DistributedReentrantLock) lock;
    }

    /**
     * Get exclusive lock.
     * 
     * @param ownerId
     * @param relativeLockPath
     * @return
     */
    public DistributedLock getLock(String ownerId, String clusterId, String lockName) {
        return getLock(PathContext.USER, ownerId, clusterId, lockName, Sovereign.DEFAULT_ACL_LIST);
    }

    /**
     * Get exclusive lock.
     * 
     * @param ownerId
     * @param relativeLockPath
     * @param aclList
     * @return
     */
    public DistributedLock getLock(PathContext pathContext, String ownerId, String clusterId, String lockName,
            List<ACL> aclList) {
        String entityPath = CoordServicePathUtil.getAbsolutePathEntity(getPathScheme(), pathContext, PathType.COORD,
                clusterId, ReservationType.LOCK_EXCLUSIVE, lockName);
        DistributedLock lock = new ZkLock(zkReservationManager, ownerId, entityPath, ReservationType.LOCK_EXCLUSIVE,
                aclList);
        this.coordinationServiceCache.putLock(entityPath, ReservationType.LOCK_EXCLUSIVE, lock);

        return lock;
    }

    /**
     * 
     * @param ownerId
     * @param relativeLockPath
     * @return
     */
    public DistributedReadWriteLock getReadWriteLock(String ownerId, String clusterId, String lockName) {
        return getReadWriteLock(PathContext.USER, ownerId, clusterId, lockName, Sovereign.DEFAULT_ACL_LIST);
    }

    /**
     * 
     * @param ownerId
     * @param relativeLockPath
     * @param aclList
     * @return
     */
    public DistributedReadWriteLock getReadWriteLock(PathContext pathContext, String ownerId, String clusterId,
            String lockName, List<ACL> aclList) {

        // write lock
        String writeEntityPath = CoordServicePathUtil.getAbsolutePathEntity(getPathScheme(), pathContext,
                PathType.COORD, clusterId, ReservationType.LOCK_EXCLUSIVE, lockName);
        DistributedLock writeLock = new ZkReentrantLock(zkReservationManager, ownerId, writeEntityPath,
                ReservationType.LOCK_EXCLUSIVE, aclList);
        this.coordinationServiceCache.putLock(writeEntityPath, ReservationType.LOCK_EXCLUSIVE, writeLock);

        // read lock
        String readEntityPath = CoordServicePathUtil.getAbsolutePathEntity(getPathScheme(), pathContext,
                PathType.COORD, clusterId, ReservationType.LOCK_SHARED, lockName);
        DistributedLock readLock = new ZkReentrantLock(zkReservationManager, ownerId, readEntityPath,
                ReservationType.LOCK_SHARED, aclList);
        this.coordinationServiceCache.putLock(readEntityPath, ReservationType.LOCK_SHARED, readLock);

        return new ZkReadWriteLock(readLock, writeLock);
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
        return getSemaphore(PathContext.USER, ownerId, clusterId, semaphoreName, new ConstantPermitPoolSize(
                permitPoolSize), Sovereign.DEFAULT_ACL_LIST);
    }

    public DistributedSemaphore getFixedSemaphore(String ownerId, String clusterId, String semaphoreName,
            int permitPoolSize, List<ACL> aclList) {
        return getSemaphore(PathContext.USER, ownerId, clusterId, semaphoreName, new ConstantPermitPoolSize(
                permitPoolSize), aclList);
    }

    /**
     * 
     * @param ownerId
     * @param clusterId
     * @param semaphoreName
     * @return
     */
    public DistributedSemaphore getConfiguredSemaphore(String ownerId, String clusterId, String semaphoreName) {
        return getConfiguredSemaphore(PathContext.USER, ownerId, clusterId, semaphoreName, -1, false,
                Sovereign.DEFAULT_ACL_LIST);
    }

    public DistributedSemaphore getConfiguredSemaphore(String ownerId, String clusterId, String semaphoreName,
            List<ACL> aclList) {
        return getConfiguredSemaphore(PathContext.USER, ownerId, clusterId, semaphoreName, -1, false, aclList);
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
        return getConfiguredSemaphore(PathContext.USER, ownerId, clusterId, semaphoreName, permitPoolSize,
                createConfigurationIfNecessary, Sovereign.DEFAULT_ACL_LIST);
    }

    public DistributedSemaphore getConfiguredSemaphore(PathContext pathContext, String ownerId, String clusterId,
            String semaphoreName, int permitPoolSize, boolean createConfigurationIfNecessary, List<ACL> aclList) {

        String entityPath = CoordServicePathUtil.getAbsolutePathEntity(getPathScheme(), PathContext.USER,
                PathType.COORD, clusterId, ReservationType.SEMAPHORE, semaphoreName);

        /**
         * create permit pool size function and add observer to adjust as
         * necessary
         **/
        Map<String, String> semaphoreConf = null;
        PermitPoolSize pps = null;
        pps = coordinationServiceCache.getPermitPoolSize(entityPath);
        if (pps == null) {
            pps = new ConfiguredPermitPoolSize(permitPoolSize);
            pps = coordinationServiceCache.putOrReturnCachedPermitPoolSize(entityPath, pps);
        } else {
            // should not happen
            if (!(pps instanceof ConfiguredPermitPoolSize)) {
                throw new IllegalStateException("PermitPoolSize already exists but is not of type "
                        + ConfiguredPermitPoolSize.class.getName() + ":  clusterId=" + clusterId + "; semaphoreName="
                        + semaphoreName);
            }
        }

        // read config
        semaphoreConf = getSemaphoreConf(pathContext, clusterId, semaphoreName, (ConfObserver<Map<String, String>>) pps);

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

        /** create and cache **/
        DistributedSemaphore semaphore = getSemaphore(pathContext, ownerId, clusterId, semaphoreName, pps, aclList);
        coordinationServiceCache.putSemaphore(entityPath, semaphore);

        return semaphore;
    }

    /**
     * @param ownerId
     * @param relativeSemaphorePath
     * @param permitPoolSizeFunction
     * @param aclList
     * @return
     */
    public DistributedSemaphore getSemaphore(PathContext pathContext, String ownerId, String clusterId,
            String semaphoreName, PermitPoolSize permitPoolSize, List<ACL> aclList) {

        String entityPath = CoordServicePathUtil.getAbsolutePathEntity(getPathScheme(), pathContext, PathType.COORD,
                clusterId, ReservationType.SEMAPHORE, semaphoreName);

        // check to see if permit pool size is already in cache
        PermitPoolSize pps = coordinationServiceCache.getPermitPoolSize(entityPath);
        if (pps == null) {
            pps = coordinationServiceCache.putOrReturnCachedPermitPoolSize(entityPath, permitPoolSize);
        } else {
            if (pps.getClass() != permitPoolSize.getClass()) {
                throw new IllegalStateException("PermitPoolSize already exists but is not of type "
                        + permitPoolSize.getClass().getName() + ":  clusterId=" + clusterId + "; semaphoreName="
                        + semaphoreName);
            }
        }

        // create and put in cache
        DistributedSemaphore semaphore = new ZkSemaphore(zkReservationManager, ownerId, entityPath, aclList, pps);
        coordinationServiceCache.putSemaphore(entityPath, semaphore);

        return semaphore;
    }

    /**
     * 
     * @param clusterId
     * @param semaphoreName
     * @param permitPoolSize
     */
    public void setSemaphoreConf(String clusterId, String semaphoreName, int permitPoolSize) {
        setSemaphoreConf(PathContext.USER, clusterId, semaphoreName, permitPoolSize, Sovereign.DEFAULT_ACL_LIST);
    }

    public void setSemaphoreConf(PathContext pathContext, String clusterId, String semaphoreName, int permitPoolSize,
            List<ACL> aclList) {
        // write configuration to ZK
        Map<String, String> semaphoreConf = new HashMap<String, String>(1, 0.9f);
        semaphoreConf.put("permitPoolSize", Integer.toString(permitPoolSize));

        // write out to ZK
        ConfService confService = (ConfService) getServiceDirectory().getService("conf");
        DataSerializer<Map<String, String>> confSerializer = new JsonDataSerializer<Map<String, String>>();
        confService.putConfAbsolutePath(pathContext, PathType.COORD, clusterId,
                getPathScheme().join(ReservationType.SEMAPHORE.category(), semaphoreName), semaphoreConf,
                confSerializer, aclList);
    }

    /**
     * 
     * @param clusterId
     * @param semaphoreName
     * @return
     */
    public Map<String, String> getSemaphoreConf(String clusterId, String semaphoreName) {
        return getSemaphoreConf(PathContext.USER, clusterId, semaphoreName, null);
    }

    Map<String, String> getSemaphoreConf(PathContext pathContext, String clusterId, String semaphoreName,
            ConfObserver<Map<String, String>> confObserver) {
        // read configuration
        ConfService confService = (ConfService) getServiceDirectory().getService("conf");
        DataSerializer<Map<String, String>> confSerializer = new JsonDataSerializer<Map<String, String>>();
        Map<String, String> semaphoreConf = confService.getConfAbsolutePath(pathContext, PathType.COORD, clusterId,
                getPathScheme().join(ReservationType.SEMAPHORE.category(), semaphoreName), confSerializer,
                confObserver, true);
        return semaphoreConf;
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
        zkReservationManager = new ZkReservationManager(getZkClient(), getPathScheme(), getPathCache(),
                coordinationServiceCache);

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
        // original path
        String path1 = event.getPath();

        // check with last token stripped out since it may be a reservation
        // node, but observers are registered to the parent lock, semaphore,
        // barrier, etc. node
        String path2 = path1.substring(0, path1.lastIndexOf('/'));

        logger.debug("filterWatchedEvent():  path1={}; path2={}", path1, path2);

        return !observerManager.isBeingObserved(path2) && !observerManager.isBeingObserved(path1);

    }

    @Override
    public void nodeDataChanged(WatchedEvent event) {
        String path = event.getPath();
    }

    // @Override
    // public void nodeChildrenChanged(WatchedEvent event) {
    // String path = event.getPath();
    // String[] tokens = getPathScheme().tokenizePath(path);
    //
    // logger.info("nodeChildrenChanged():  path={}", path);
    //
    // // see if it is the primary lock, semaphore, barrier node that has
    // // changed
    // if (tokens.length > 1) {
    // ReservationType reservationType =
    // ReservationType.fromCategory(tokens[tokens.length - 1]);
    // if (reservationType != null) {
    // List<String> sortedReservationList =
    // zkReservationManager.getSortedReservationList(path, true);
    // observerManager.signal(event.getPath(), sortedReservationList);
    // }
    // }
    // }

    @Override
    public void nodeDeleted(WatchedEvent event) {
        String path = event.getPath();
        String[] tokens = getPathScheme().tokenizePath(path);

        logger.debug("nodeDeleted():  path={}", path);

        // see if it is the primary lock, semaphore, barrier node that has been
        // deleted
        if (tokens.length > 1 && ReservationType.fromCategory(tokens[tokens.length - 1]) != null) {
            // primary node was deleted so state is unknown
            observerManager.signalStateUnknown(null);
        }

        // check to see if we need to signal that a lock holder was deleted
        observerManager.signal(path.substring(0, path.lastIndexOf('/')), path);
    }

    private static abstract class CoordObserverWrapper<T extends ServiceObserver> extends ServiceObserverWrapper<T> {
        private final String entityPath;
        private final CoordinationServiceCache coordinationServiceCache;

        protected CoordObserverWrapper(String entityPath, CoordinationServiceCache coordinationServiceCache) {
            this.entityPath = entityPath;
            this.coordinationServiceCache = coordinationServiceCache;
        }

        public String getEntityPath() {
            return entityPath;
        }

        public CoordinationServiceCache getCoordinationServiceCache() {
            return coordinationServiceCache;
        }

    }

    private static class LockObserverWrapper extends CoordObserverWrapper<LockObserver> {

        public LockObserverWrapper(String entityPath, LockObserver observer,
                CoordinationServiceCache coordinationServiceCache) {
            super(entityPath, coordinationServiceCache);
            this.setObserver(observer);
        }

        @Override
        public void signalObserver(Object o) {
            String revokedReservationId = (String) o;

            Collection<DistributedLock> exclusiveLocks = getCoordinationServiceCache().getLocks(getEntityPath(),
                    ReservationType.LOCK_EXCLUSIVE);
            checkForRevoked(exclusiveLocks, revokedReservationId);

            Collection<DistributedLock> sharedLocks = getCoordinationServiceCache().getLocks(getEntityPath(),
                    ReservationType.LOCK_SHARED);
            checkForRevoked(sharedLocks, revokedReservationId);
        }

        void checkForRevoked(Collection<DistributedLock> locks, String revokedReservationId) {
            for (DistributedLock lock : locks) {
                // logger.debug("Checking:  lockId={}; revokedReservationId={}",
                // lock.getLockId(), revokedReservationId);
                if (revokedReservationId.equals(lock.getLockId())) {
                    lock.revoke(revokedReservationId);
                    this.getObserver().revoked(lock, revokedReservationId);
                }
            }
        }
    }// class

    private static class SemaphoreObserverWrapper extends CoordObserverWrapper<SemaphoreObserver> {

        private final PathScheme pathScheme;

        public SemaphoreObserverWrapper(String entityPath, SemaphoreObserver observer,
                CoordinationServiceCache coordinationServiceCache, PathScheme pathScheme) {
            super(entityPath, coordinationServiceCache);
            this.setObserver(observer);
            this.pathScheme = pathScheme;
        }

        @Override
        public void signalObserver(Object o) {
            String revokedReservationId = (String) o;
            Collection<DistributedSemaphore> semaphores = getCoordinationServiceCache().getSemaphores(getEntityPath());

            // create set and then check acquired permit ids against it
            // Set<String> permitIdSet = new
            // HashSet<String>(sortedReservationList.size(), 0.9f);
            // for (String reservationId : sortedReservationList) {
            // permitIdSet.add(pathScheme.join(getEntityPath(), reservationId));
            // }

            // check and signal observers of any revocations
            for (DistributedSemaphore semaphore : semaphores) {
                // check and see which ones have been revoked
                if (semaphore.getAcquiredPermitIds().contains(revokedReservationId)) {
                    semaphore.revoke(revokedReservationId);
                    this.getObserver().revoked(semaphore, revokedReservationId);
                }// for
            }// for

        }
    }// class
}
