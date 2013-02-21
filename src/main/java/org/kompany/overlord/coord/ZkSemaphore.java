package org.kompany.overlord.coord;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.kompany.overlord.PathContext;
import org.kompany.overlord.util.TimeUnitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains general semantics of {@link java.util.concurrent.Semaphore} with
 * some exceptions. In this implementation, permit granting is always fair.
 * 
 * @author ypai
 * 
 */
public class ZkSemaphore implements Watcher {

    private static final Logger logger = LoggerFactory.getLogger(ZkSemaphore.class);

    private final ZkLockManager zkLockManager;
    private final String ownerId;
    private final PathContext pathContext;
    private final String relativeSemaphorePath;
    // private final ReservationType lockType;
    private final List<ACL> aclList;
    private volatile int maxAvailablePermits;

    private final Set<String> acquiredPermitPathSet = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>(
            16, 0.9f, 2));

    public ZkSemaphore(ZkLockManager zkLockManager, String ownerId, PathContext pathContext,
            String relativeSemaphorePath, List<ACL> aclList, int maxAvailablePermits) {
        super();
        this.zkLockManager = zkLockManager;
        this.ownerId = ownerId;
        this.pathContext = pathContext;
        this.relativeSemaphorePath = relativeSemaphorePath;
        this.aclList = aclList;
        this.maxAvailablePermits = maxAvailablePermits;
    }

    public void acquire() throws InterruptedException {
        String acquiredPermitPath = zkLockManager.acquire(ownerId, pathContext, relativeSemaphorePath,
                ReservationType.PERMIT, maxAvailablePermits, aclList, -1, true);
        if (acquiredPermitPath != null) {
            acquiredPermitPathSet.add(acquiredPermitPath);
        }
    }

    public void acquire(int permits) throws InterruptedException {
        Set<String> tmpAcquiredPermitPathSet = new HashSet<String>(permits, 0.9f);

        try {
            for (int i = 0; i < permits; i++) {
                String acquiredPermitPath = zkLockManager.acquire(ownerId, pathContext, relativeSemaphorePath,
                        ReservationType.PERMIT, maxAvailablePermits, aclList, -1, true);
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

    public void acquireUninterruptibly() {
        // keep acquiring permit until done and then interrupt current thread if
        // necessary
        boolean interrupted = false;
        try {
            String acquiredPermitPath = zkLockManager.acquire(ownerId, pathContext, relativeSemaphorePath,
                    ReservationType.PERMIT, maxAvailablePermits, aclList, -1, false);

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

    public void acquireUninterruptibly(int permits) {
        // keep acquiring permits until done and then interrupt current thread
        // if necessary
        boolean interrupted = false;
        while (permits > 0) {
            try {
                String acquiredPermitPath = zkLockManager.acquire(ownerId, pathContext, relativeSemaphorePath,
                        ReservationType.PERMIT, maxAvailablePermits, aclList, -1, false);

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

    public int availablePermits() {
        int currentlyAvailable = this.maxAvailablePermits - getQueuedReservations().size();
        return currentlyAvailable < 0 ? 0 : currentlyAvailable;

    }

    /**
     * Tries to acquire as many permits as possible without waiting, or until
     * interruption. On interruption, keeps permits that it has already
     * acquired.
     * 
     * @return
     */
    public int drainPermits() {
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
                acquiredPermitPath = zkLockManager.acquire(ownerId, pathContext, relativeSemaphorePath,
                        ReservationType.PERMIT, maxAvailablePermits, aclList, 0, false);

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

    protected Collection<String> getQueuedReservations() {
        return zkLockManager.getReservationList(pathContext, relativeSemaphorePath, ReservationType.PERMIT, true);
    }

    public boolean isFair() {
        return true;
    }

    protected void reducePermits(int permitReduction) {
        if (permitReduction < 0) {
            throw new IllegalArgumentException("Argument permitReduction cannot be less than zero.");
        }

        this.maxAvailablePermits = maxAvailablePermits - permitReduction;
    }

    public void release() {
        int permitsReleased = 0;
        for (String acquiredPermitPath : acquiredPermitPathSet) {
            if (zkLockManager.relinquish(acquiredPermitPath)) {
                acquiredPermitPathSet.remove(acquiredPermitPath);
                permitsReleased++;
                if (permitsReleased >= 1) {
                    break;
                }
            }
        }

        if (permitsReleased < 1) {
            logger.warn("Number of permits released does not match requested:  requested={}; released={}", 1,
                    permitsReleased);
        }
    }

    public void release(int permitsToRelease) {
        if (permitsToRelease < 0) {
            throw new IllegalArgumentException("Argument permitsToRelease cannot be less than zero.");
        }

        int permitsReleased = 0;
        for (String acquiredPermitPath : acquiredPermitPathSet) {
            if (zkLockManager.relinquish(acquiredPermitPath)) {
                acquiredPermitPathSet.remove(acquiredPermitPath);
                permitsReleased++;
                if (permitsReleased >= permitsToRelease) {
                    break;
                }
            }
        }

        if (permitsReleased < permitsToRelease) {
            logger.warn("Number of permits released does not match requested:  requested={}; released={}",
                    permitsToRelease, permitsReleased);
        }
    }

    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return super.toString();
    }

    /**
     * Unlike java.util.concurrent.Semaphore: this implementation of
     * tryAcquire() does not "barge in" and is always fair.
     * 
     * @return
     */
    public boolean tryAcquire() {
        if (!isPermitAllocationAvailable(1)) {
            return false;
        }

        try {
            String acquiredPermitPath = zkLockManager.acquire(ownerId, pathContext, relativeSemaphorePath,
                    ReservationType.PERMIT, maxAvailablePermits, aclList, 0, false);

            if (acquiredPermitPath != null) {
                acquiredPermitPathSet.add(acquiredPermitPath);
            }

            return true;
        } catch (InterruptedException e) {
            logger.warn("Interrupted in tryAcquire():  " + e, e);
            return false;
        }
    }

    public boolean tryAcquire(int permits, long wait, TimeUnit timeUnit) throws InterruptedException {
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
                String acquiredPermitPath = zkLockManager.acquire(ownerId, pathContext, relativeSemaphorePath,
                        ReservationType.PERMIT, maxAvailablePermits, aclList, timeWaitMillis, true);
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

    public boolean tryAcquire(int permits) {
        if (!isPermitAllocationAvailable(permits)) {
            return false;
        }

        // attempt to acquire all permits, returning all temporarily acquired
        // permits if we to get desired number of permits quickly
        Set<String> tmpAcquiredPermitPathSet = new HashSet<String>(permits, 0.9f);

        try {
            for (int i = 0; i < permits; i++) {
                String acquiredPermitPath = zkLockManager.acquire(ownerId, pathContext, relativeSemaphorePath,
                        ReservationType.PERMIT, maxAvailablePermits, aclList, 0, true);
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

    public boolean tryAcquire(long wait, TimeUnit timeUnit) throws InterruptedException {
        if (!isPermitAllocationAvailable(1)) {
            return false;
        }

        long timeWaitMillis = TimeUnitUtils.toMillis(wait, timeUnit);

        String acquiredPermitPath = zkLockManager.acquire(ownerId, pathContext, relativeSemaphorePath,
                ReservationType.PERMIT, maxAvailablePermits, aclList, timeWaitMillis, false);

        if (acquiredPermitPath != null) {
            acquiredPermitPathSet.add(acquiredPermitPath);
        } else {
            return false;
        }

        return true;
    }

    @Override
    public void process(WatchedEvent event) {
        // log if DEBUG
        if (logger.isDebugEnabled()) {
            logger.debug("***** Received ZooKeeper Event:  {}",
                    ReflectionToStringBuilder.toString(event, ToStringStyle.DEFAULT_STYLE));

        }

        // we only care about events having to do with this semaphore
        String path = event.getPath();
        if (path == null || !path.endsWith(this.relativeSemaphorePath)) {
            return;
        }

        // process events
        switch (event.getType()) {
        case NodeCreated:

        case NodeChildrenChanged:
        case NodeDataChanged:
            // TODO: read config

            break;
        case NodeDeleted:
            this.maxAvailablePermits = 0;
            break;

        case None:
            Event.KeeperState eventState = event.getState();
            if (eventState == Event.KeeperState.SyncConnected) {
                // TODO: update configuration

            } else if (eventState == Event.KeeperState.Disconnected || eventState == Event.KeeperState.Expired) {
                // disconnected so set available permits to 0
                this.maxAvailablePermits = 0;

            } else {
                logger.warn("Unhandled event state:  "
                        + ReflectionToStringBuilder.toString(event, ToStringStyle.DEFAULT_STYLE));
            }
            break;

        default:
            logger.warn("Unhandled event type:  "
                    + ReflectionToStringBuilder.toString(event, ToStringStyle.DEFAULT_STYLE));
        }

    }// process()

    boolean isPermitAllocationAvailable(int permits) {
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
