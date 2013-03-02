package org.kompany.overlord.coord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.data.ACL;
import org.kompany.overlord.PathContext;
import org.kompany.overlord.util.TimeUnitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains general semantics of {@link java.util.concurrent.Semaphore} with
 * some exceptions. In this implementation, permit granting is first come, first
 * served.
 * 
 * @author ypai
 * 
 */
public class ZkSemaphore implements DistributedSemaphore {

    private static final Logger logger = LoggerFactory.getLogger(ZkSemaphore.class);

    private final ZkLockManager zkLockManager;
    private final String ownerId;
    private final PathContext pathContext;
    private final String relativeSemaphorePath;
    // private final ReservationType lockType;
    private final List<ACL> aclList;
    private final PermitPoolSize permitPoolSizeFunction;

    private final Set<String> acquiredPermitPathSet = new HashSet<String>(16, 0.9f);

    public ZkSemaphore(ZkLockManager zkLockManager, String ownerId, PathContext pathContext,
            String relativeSemaphorePath, List<ACL> aclList, PermitPoolSize permitPoolSizeFunction) {
        super();
        this.zkLockManager = zkLockManager;
        this.ownerId = ownerId;
        this.pathContext = pathContext;
        this.relativeSemaphorePath = relativeSemaphorePath;
        this.aclList = aclList;
        this.permitPoolSizeFunction = permitPoolSizeFunction;
    }

    public synchronized Set<String> getAcquiredPermitIds() {
        return Collections.unmodifiableSet(acquiredPermitPathSet);
    }

    @Override
    public synchronized int permitPoolSize() {
        return this.permitPoolSizeFunction.get();
    }

    @Override
    public synchronized void acquire() throws InterruptedException {
        String acquiredPermitPath = zkLockManager.acquireForSemaphore(ownerId, pathContext, relativeSemaphorePath,
                ReservationType.SEMAPHORE, permitPoolSize(), aclList, -1, true);
        if (acquiredPermitPath != null) {
            acquiredPermitPathSet.add(acquiredPermitPath);
        }
    }

    @Override
    public synchronized void acquire(int permits) throws InterruptedException {
        Set<String> tmpAcquiredPermitPathSet = new HashSet<String>(permits, 0.9f);

        try {
            for (int i = 0; i < permits; i++) {
                String acquiredPermitPath = zkLockManager.acquireForSemaphore(ownerId, pathContext,
                        relativeSemaphorePath, ReservationType.SEMAPHORE, permitPoolSize(), aclList, -1, true);
                if (acquiredPermitPath != null) {
                    tmpAcquiredPermitPathSet.add(acquiredPermitPath);
                }
            }
            acquiredPermitPathSet.addAll(tmpAcquiredPermitPathSet);
        } catch (InterruptedException e) {
            // return all permits acquired thus far in method
            for (String acquiredPermitPath : tmpAcquiredPermitPathSet) {
                zkLockManager.relinquish(acquiredPermitPath);
            }

            throw e;
        }
    }

    public synchronized void acquireUninterruptibly() {
        // keep acquiring permit until done and then interrupt current thread if
        // necessary
        boolean interrupted = false;
        try {
            String acquiredPermitPath = zkLockManager.acquireForSemaphore(ownerId, pathContext, relativeSemaphorePath,
                    ReservationType.SEMAPHORE, permitPoolSize(), aclList, -1, false);

            if (acquiredPermitPath != null) {
                acquiredPermitPathSet.add(acquiredPermitPath);
            }
        } catch (InterruptedException e) {
            logger.warn("Interrupted in acquireUninterruptibly():  should not happen:  " + e, e);
            interrupted = true;
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    public synchronized void acquireUninterruptibly(int permits) {
        // keep acquiring permits until done and then interrupt current thread
        // if necessary
        boolean interrupted = false;
        while (permits > 0) {
            try {
                String acquiredPermitPath = zkLockManager.acquireForSemaphore(ownerId, pathContext,
                        relativeSemaphorePath, ReservationType.SEMAPHORE, permitPoolSize(), aclList, -1, false);

                if (acquiredPermitPath != null) {
                    acquiredPermitPathSet.add(acquiredPermitPath);
                    permits--;
                }
            } catch (InterruptedException e) {
                logger.warn("Interrupted in acquireUninterruptibly():  should not happen:  " + e, e);
                interrupted = true;
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }

    }

    public synchronized int availablePermits() {
        int currentlyAvailable = permitPoolSize() - getAllPermitRequests().size();
        return currentlyAvailable < 0 ? 0 : currentlyAvailable;

    }

    /**
     * Tries to acquire as many permits as possible without waiting, or until
     * interruption. On interruption, keeps permits that it has already
     * acquired.
     * 
     * @return
     */
    public synchronized int drainPermits() {
        if (!isPermitAllocationAvailable(1)) {
            return 0;
        }

        int permitsAcquired = 0;
        boolean interrupted = false;

        // keep acquiring permits until done and then interrupt current thread
        // if necessary
        try {
            String acquiredPermitPath = null;
            do {
                acquiredPermitPath = zkLockManager.acquireForSemaphore(ownerId, pathContext, relativeSemaphorePath,
                        ReservationType.SEMAPHORE, permitPoolSize(), aclList, 0, false);

                if (acquiredPermitPath != null) {
                    acquiredPermitPathSet.add(acquiredPermitPath);
                    permitsAcquired++;
                }
            } while (acquiredPermitPath != null);
        } catch (InterruptedException e) {
            logger.warn("Interrupted in acquireUninterruptibly():  should not happen:  " + e, e);
            interrupted = true;
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }

        return permitsAcquired;

    }

    /**
     * 
     * @return the current list of permit holders and those waiting for a
     *         permit.
     */
    protected synchronized List<String> getAllPermitRequests() {
        return zkLockManager.getReservationList(pathContext, relativeSemaphorePath, ReservationType.SEMAPHORE, true);
    }

    public synchronized boolean isFair() {
        return true;
    }

    @Override
    public synchronized void release() {
        release(1);
    }

    @Override
    public synchronized void release(int permitsToRelease) {
        if (permitsToRelease < 0) {
            throw new IllegalArgumentException("Argument permitsToRelease cannot be less than zero.");
        }

        // release from ZK
        int permitsReleased = 0;
        List<String> permitIdsReleased = new ArrayList<String>(permitsToRelease);
        for (String acquiredPermitPath : acquiredPermitPathSet) {
            if (zkLockManager.relinquish(acquiredPermitPath)) {
                permitIdsReleased.add(acquiredPermitPath);
                permitsReleased++;
                if (permitsReleased >= permitsToRelease) {
                    break;
                }
            }
        }

        // remove from local set
        for (String releasedPermitId : permitIdsReleased) {
            acquiredPermitPathSet.remove(releasedPermitId);
        }

        if (permitsReleased < permitsToRelease) {
            logger.warn("Number of permits released does not match requested:  requested={}; released={}",
                    permitsToRelease, permitsReleased);
        }
    }

    @Override
    public synchronized String toString() {
        // TODO Auto-generated method stub
        return super.toString();
    }

    /**
     * Unlike java.util.concurrent.Semaphore: this implementation of
     * tryAcquire() does not "barge in" and is always fair.
     * 
     * @return
     */
    public synchronized boolean tryAcquire() {
        if (!isPermitAllocationAvailable(1)) {
            return false;
        }

        try {
            String acquiredPermitPath = zkLockManager.acquireForSemaphore(ownerId, pathContext, relativeSemaphorePath,
                    ReservationType.SEMAPHORE, permitPoolSize(), aclList, 0, false);

            if (acquiredPermitPath != null) {
                acquiredPermitPathSet.add(acquiredPermitPath);
            }

            return true;
        } catch (InterruptedException e) {
            logger.warn("Interrupted in tryAcquire():  " + e, e);
            return false;
        }
    }

    public synchronized boolean tryAcquire(int permits, long wait, TimeUnit timeUnit) throws InterruptedException {
        if (!isPermitAllocationAvailable(permits)) {
            return false;
        }

        // attempt to acquire all permits, returning all temporarily acquired
        // permits if we to get desired number of permits quickly
        Set<String> tmpAcquiredPermitPathSet = new HashSet<String>(permits, 0.9f);
        long timeWaitMillis = TimeUnitUtils.toMillis(wait, timeUnit);
        long startTimestamp = System.currentTimeMillis();
        try {
            for (int i = 0; i < permits && System.currentTimeMillis() - startTimestamp < timeWaitMillis; i++) {
                String acquiredPermitPath = zkLockManager.acquireForSemaphore(ownerId, pathContext,
                        relativeSemaphorePath, ReservationType.SEMAPHORE, permitPoolSize(), aclList, timeWaitMillis,
                        true);
                if (acquiredPermitPath != null) {
                    tmpAcquiredPermitPathSet.add(acquiredPermitPath);
                }
            }

            // check that all permits were acquired; if not, return all
            if (tmpAcquiredPermitPathSet.size() < permits) {
                // return all permits acquired thus far in method
                for (String tmpAcquiredPermitPath : tmpAcquiredPermitPathSet) {
                    zkLockManager.relinquish(tmpAcquiredPermitPath);
                }
                return false;
            }

            if (tmpAcquiredPermitPathSet.size() > 0) {
                acquiredPermitPathSet.addAll(tmpAcquiredPermitPathSet);
            }

            return true;
        } catch (InterruptedException e) {
            // return all permits acquired thus far in method
            for (String acquiredPermitPath : tmpAcquiredPermitPathSet) {
                zkLockManager.relinquish(acquiredPermitPath);
            }

            return false;
        }

    }

    public synchronized boolean tryAcquire(int permits) {
        if (!isPermitAllocationAvailable(permits)) {
            return false;
        }

        // attempt to acquire all permits, returning all temporarily acquired
        // permits if we to get desired number of permits quickly
        Set<String> tmpAcquiredPermitPathSet = new HashSet<String>(permits, 0.9f);

        try {
            for (int i = 0; i < permits; i++) {
                String acquiredPermitPath = zkLockManager.acquireForSemaphore(ownerId, pathContext,
                        relativeSemaphorePath, ReservationType.SEMAPHORE, permitPoolSize(), aclList, 0, true);
                if (acquiredPermitPath != null) {
                    tmpAcquiredPermitPathSet.add(acquiredPermitPath);
                } else {
                    // return all permits acquired thus far in method
                    for (String tmpAcquiredPermitPath : tmpAcquiredPermitPathSet) {
                        zkLockManager.relinquish(tmpAcquiredPermitPath);
                    }
                    return false;
                }
            }
            if (tmpAcquiredPermitPathSet.size() > 0) {
                acquiredPermitPathSet.addAll(tmpAcquiredPermitPathSet);
            }

            return true;
        } catch (InterruptedException e) {
            // return all permits acquired thus far in method
            for (String acquiredPermitPath : tmpAcquiredPermitPathSet) {
                zkLockManager.relinquish(acquiredPermitPath);
            }

            return false;
        }
    }

    public synchronized boolean tryAcquire(long wait, TimeUnit timeUnit) throws InterruptedException {
        if (!isPermitAllocationAvailable(1)) {
            return false;
        }

        long timeWaitMillis = TimeUnitUtils.toMillis(wait, timeUnit);

        String acquiredPermitPath = zkLockManager.acquireForSemaphore(ownerId, pathContext, relativeSemaphorePath,
                ReservationType.SEMAPHORE, permitPoolSize(), aclList, timeWaitMillis, false);

        if (acquiredPermitPath != null) {
            acquiredPermitPathSet.add(acquiredPermitPath);
        } else {
            return false;
        }

        return true;
    }

    // @Override
    // public void process(WatchedEvent event) {
    // // log if DEBUG
    // if (logger.isDebugEnabled()) {
    // logger.debug("***** Received ZooKeeper Event:  {}",
    // ReflectionToStringBuilder.toString(event, ToStringStyle.DEFAULT_STYLE));
    //
    // }
    //
    // // we only care about events having to do with this semaphore
    // String path = event.getPath();
    // if (path == null || !path.endsWith(this.relativeSemaphorePath)) {
    // return;
    // }
    //
    // // process events
    // switch (event.getType()) {
    // case NodeCreated:
    //
    // case NodeChildrenChanged:
    // case NodeDataChanged:
    // // TODO: read config
    //
    // break;
    // case NodeDeleted:
    // this.permitPoolSize = 0;
    // break;
    //
    // case None:
    // Event.KeeperState eventState = event.getState();
    // if (eventState == Event.KeeperState.SyncConnected) {
    // // TODO: update configuration
    //
    // } else if (eventState == Event.KeeperState.Disconnected || eventState ==
    // Event.KeeperState.Expired) {
    // // disconnected so set available permits to 0
    // this.permitPoolSize = 0;
    //
    // } else {
    // logger.warn("Unhandled event state:  "
    // + ReflectionToStringBuilder.toString(event,
    // ToStringStyle.DEFAULT_STYLE));
    // }
    // break;
    //
    // default:
    // logger.warn("Unhandled event type:  "
    // + ReflectionToStringBuilder.toString(event,
    // ToStringStyle.DEFAULT_STYLE));
    // }
    //
    // }// process()

    synchronized boolean isPermitAllocationAvailable(int permits) {
        if (permits < 0) {
            throw new IllegalArgumentException("Argument permits cannot be less than zero.");
        }

        // return immediately if we know there are not enough permits available
        if (permits > availablePermits()) {
            return false;
        }

        return true;
    }
}
