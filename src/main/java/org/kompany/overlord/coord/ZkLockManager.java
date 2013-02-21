package org.kompany.overlord.coord;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.kompany.overlord.PathContext;
import org.kompany.overlord.PathScheme;
import org.kompany.overlord.PathType;
import org.kompany.overlord.ZkClient;
import org.kompany.overlord.util.PathCache;
import org.kompany.overlord.util.PathCache.PathCacheEntry;
import org.kompany.overlord.util.ZkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ZkLockManager {

    private static final Logger logger = LoggerFactory.getLogger(ZkLockManager.class);

    private final Comparator<String> lockReservationComparator = new LockReservationComparator("_");
    private final ZkClient zkClient;
    private final PathScheme pathScheme;
    private final ZkUtil zkUtil = new ZkUtil();
    private volatile boolean shutdown = false;

    private final PathCache pathCache;

    ZkLockManager(ZkClient zkClient, PathScheme pathScheme, PathCache pathCache) {
        super();
        this.zkClient = zkClient;
        this.pathScheme = pathScheme;
        this.pathCache = pathCache;
    }

    public void shutdown() {
        this.shutdown = true;
    }

    public List<String> getReservationList(PathContext pathContext, String entityName, ReservationType reservationType,
            boolean useCache) {
        String entityPath = pathScheme.getAbsolutePath(pathContext, PathType.COORD,
                reservationType.getSubCategoryPathToken() + "/" + entityName);
        try {
            List<String> lockReservationList = null;

            PathCacheEntry pathCacheEntry = pathCache.get(entityPath);
            if (useCache && pathCacheEntry != null) {
                // found in cache
                lockReservationList = pathCacheEntry.getChildren();
            } else {
                lockReservationList = zkClient.getChildren(entityPath, true);
            }

            return lockReservationList;
        } catch (KeeperException e) {
            throw new IllegalStateException("Error trying to get reservation list:  " + e + ": entityPath="
                    + entityPath, e);
        } catch (Exception e) {
            throw new IllegalStateException("Error trying to get reservation list:  " + e + ": entityPath="
                    + entityPath, e);
        }
    }

    /**
     * 
     * @param ownerId
     * @param pathContext
     * @param entityName
     * @param reservationType
     * @param totalAvailable
     *            the total number of locks available; -1 for no limit
     * @param aclList
     * @param waitTimeoutMs
     *            total time to wait in millis for lock acquisition; -1 to wait
     *            indefinitely
     * @param aclList
     * @param interruptible
     *            true to throw InterruptedException(s); false to eat them
     * @return path of acquired lock node (this needs to be kept in order to
     *         unlock!)
     */
    public String acquire(String ownerId, PathContext pathContext, String entityName, ReservationType reservationType,
            int totalAvailable, List<ACL> aclList, long waitTimeoutMs, boolean interruptible)
            throws InterruptedException {
        // long minSleep = waitTimeoutMs / 100 < 100 ? 100 : waitTimeoutMs /
        // 100;
        try {
            long startTimestamp = System.currentTimeMillis();
            // String lockReservationPath = null;
            // LockWatcher lockWatcher = null;

            // path to lock (parent node of all reservations)
            String lockPath = pathScheme.getAbsolutePath(pathContext, PathType.COORD,
                    reservationType.getSubCategoryPathToken() + "/" + entityName);

            // owner data in JSON
            String lockReservationData = "{\"ownerId\":\"" + ownerId + "\"}";

            // path to lock reservation node (to "get in line" for lock)
            String lockReservationPrefix = pathScheme.getAbsolutePath(pathContext, PathType.COORD,
                    reservationType.getSubCategoryPathToken() + "/" + entityName + "/" + reservationType + "_");

            // create lock reservation sequential node
            String lockReservationPath = zkUtil.updatePath(zkClient, pathScheme, lockReservationPrefix,
                    lockReservationData.getBytes("UTF-8"), aclList, CreateMode.EPHEMERAL_SEQUENTIAL, -1);

            // path token (last part of path)
            String lockReservation = lockReservationPath.substring(lockReservationPath.lastIndexOf('/') + 1);

            // create lock watcher for wait/notify
            if (logger.isDebugEnabled()) {
                logger.debug("Attempting to acquire:  ownerId={}; lockType={}; lockReservationPath={}", new Object[] {
                        ownerId, reservationType, lockReservationPath });
            }
            ZkLockWatcher lockReservationWatcher = new ZkLockWatcher(lockPath, lockReservationPath);
            String acquiredLockPath = null;
            do {
                try {
                    // get lock reservation list
                    List<String> lockReservationList = zkClient.getChildren(lockPath, lockReservationWatcher);

                    // sort child list
                    Collections.sort(lockReservationList, lockReservationComparator);

                    // if we are in the first N elements, we have the lock
                    for (int i = 0; (totalAvailable < 0 || i < totalAvailable) && i < lockReservationList.size(); i++) {
                        String currentReservation = lockReservationList.get(i);

                        // see if we have the lock
                        if (lockReservation.equals(currentReservation)) {
                            if (i == 0 || reservationType != ReservationType.EXCLUSIVE) {
                                acquiredLockPath = lockReservationPath;
                                break;
                            }
                        } else {
                            // if we do not own lock and it is exclusive, skip
                            // the rest
                            if (i == 0 && currentReservation.startsWith(ReservationType.EXCLUSIVE.toString())) {
                                break;
                            }
                        }

                        // if trying to acquire an exclusive lock, no need to
                        // check after first element
                        if (reservationType == ReservationType.EXCLUSIVE && i > 0) {
                            break;
                        }
                    }

                    // wait to acquire if not yet acquired
                    if (acquiredLockPath == null) {
                        logger.info("Waiting to acquire:  ownerId={}; lockType={}; lockReservationPath={}",
                                new Object[] { ownerId, reservationType, lockReservationPath });
                        lockReservationWatcher.waitForEvent(waitTimeoutMs);
                    } else {
                        logger.info("Acquired:  ownerId={}; lockType={}; acquiredLockPath={}", new Object[] { ownerId,
                                reservationType, acquiredLockPath });
                        break;
                    }
                } catch (InterruptedException e) {
                    if (interruptible) {
                        throw e;
                    } else {
                        logger.info(
                                "Ignoring attempted interrupt while waiting for lock acquisition:  ownerId={}; lockType={}; lockPath={}",
                                new Object[] { ownerId, reservationType, lockPath });
                    }
                }

            } while (!this.shutdown && acquiredLockPath == null
                    && (waitTimeoutMs == -1 || startTimestamp + waitTimeoutMs > System.currentTimeMillis()));

            if (acquiredLockPath == null) {
                logger.info("Could not acquire:  ownerId={}; lockType={}; lockPath={}", new Object[] { ownerId,
                        reservationType, lockPath });
            }

            return acquiredLockPath;

        } catch (KeeperException e) {
            logger.error("Error trying to acquire:  " + e + ":  ownerId=" + ownerId + "; lockName=" + entityName
                    + "; lockType=" + reservationType, e);
        } catch (Exception e) {
            logger.error("Error trying to acquire:  " + e + ":  ownerId=" + ownerId + "; lockName=" + entityName
                    + "; lockType=" + reservationType, e);
        }

        return null;

    }

    public boolean relinquish(String reservationPath) {
        if (reservationPath == null) {
            throw new IllegalArgumentException("Trying to delete ZK reservation node with invalid path:  path="
                    + reservationPath);
        }// if

        try {
            logger.debug("Relinquishing:  path={}", reservationPath);

            zkClient.delete(reservationPath, -1);

            logger.info("Relinquished:  path={}", reservationPath);
            return true;
        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NONODE) {
                // already deleted, so just log
                if (logger.isDebugEnabled()) {
                    logger.debug("Already deleted ZK reservation node:  " + e + "; path=" + reservationPath, e);
                }

            } else {
                logger.error("Error while deleting ZK reservation node:  " + e + "; path=" + reservationPath, e);
                throw new IllegalStateException("Error while deleting ZK reservation node:  " + e + "; path="
                        + reservationPath, e);
            }
        } catch (Exception e) {
            logger.error("Error while deleting ZK reservation node:  " + e + "; path=" + reservationPath, e);
            throw new IllegalStateException("Error while deleting ZK reservation node:  " + e + "; path="
                    + reservationPath, e);
        }// try

        return false;
    }// unlock

}
