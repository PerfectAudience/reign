package org.kompany.overlord.coord;

import java.util.ArrayList;
import java.util.Collection;
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

    private final ZkReservationManager zkReservationManager;
    private final String ownerId;
    private final PathContext pathContext;
    private final String clusterId;
    private final String semaphoreName;
    // private final ReservationType lockType;
    private final List<ACL> aclList;
    private final PermitPoolSize permitPoolSizeFunction;

    private final Set<String> acquiredPermitIds = new HashSet<String>(16, 0.9f);

    public ZkSemaphore(ZkReservationManager zkReservationManager, String ownerId, PathContext pathContext,
            String clusterId, String semaphoreName, List<ACL> aclList, PermitPoolSize permitPoolSizeFunction) {
        super();
        this.zkReservationManager = zkReservationManager;
        this.ownerId = ownerId;
        this.pathContext = pathContext;
        this.clusterId = clusterId;
        this.semaphoreName = semaphoreName;
        this.aclList = aclList;
        this.permitPoolSizeFunction = permitPoolSizeFunction;
    }

    public synchronized Collection<String> getAcquiredPermits() {
        return Collections.unmodifiableSet(acquiredPermitIds);
    }

    @Override
    public synchronized int permitPoolSize() {
        return this.permitPoolSizeFunction.get();
    }

    @Override
    public synchronized String acquire() throws InterruptedException {
        String acquiredPermitPath = zkReservationManager.acquireForSemaphore(ownerId, pathContext, clusterId,
                semaphoreName, ReservationType.SEMAPHORE, permitPoolSize(), aclList, -1, true);
        if (acquiredPermitPath != null) {
            acquiredPermitIds.add(acquiredPermitPath);
        }

        return acquiredPermitPath;
    }

    @Override
    public synchronized Collection<String> acquire(int permits) throws InterruptedException {
        List<String> tmpAcquiredPermits = new ArrayList<String>(permits);

        try {
            for (int i = 0; i < permits; i++) {
                String acquiredPermitPath = zkReservationManager.acquireForSemaphore(ownerId, pathContext, clusterId,
                        semaphoreName, ReservationType.SEMAPHORE, permitPoolSize(), aclList, -1, true);
                if (acquiredPermitPath != null) {
                    tmpAcquiredPermits.add(acquiredPermitPath);
                }
            }
            acquiredPermitIds.addAll(tmpAcquiredPermits);
        } catch (InterruptedException e) {
            // return all permits acquired thus far in method
            for (String acquiredPermitPath : tmpAcquiredPermits) {
                zkReservationManager.relinquish(acquiredPermitPath);
            }

            acquiredPermitIds.removeAll(tmpAcquiredPermits);

            throw e;
        }

        return tmpAcquiredPermits.size() > 0 ? tmpAcquiredPermits : Collections.EMPTY_LIST;
    }

    @Override
    public synchronized boolean isValid(String permitId) {
        return acquiredPermitIds.contains(permitId);
    }

    @Override
    public synchronized void release(String permitId) {
        if (zkReservationManager.relinquish(permitId)) {
            acquiredPermitIds.remove(permitId);
        }
    }

    @Override
    public synchronized void release(Collection<String> permitIds) {
        int permitsReleased = 0;
        for (String permitId : permitIds) {
            if (zkReservationManager.relinquish(permitId)) {
                acquiredPermitIds.remove(permitId);
                permitsReleased++;
            }
        }

        // sanity check
        if (permitsReleased < permitIds.size()) {
            logger.warn("Number of permits released does not match requested:  requested={}; released={}",
                    permitIds.size(), permitsReleased);
        }
    }

    public synchronized void acquireUninterruptibly() {
        // keep acquiring permit until done and then interrupt current thread if
        // necessary
        boolean interrupted = false;
        try {
            String acquiredPermitPath = zkReservationManager.acquireForSemaphore(ownerId, pathContext, clusterId,
                    semaphoreName, ReservationType.SEMAPHORE, permitPoolSize(), aclList, -1, false);

            if (acquiredPermitPath != null) {
                acquiredPermitIds.add(acquiredPermitPath);
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
                String acquiredPermitPath = zkReservationManager.acquireForSemaphore(ownerId, pathContext, clusterId,
                        semaphoreName, ReservationType.SEMAPHORE, permitPoolSize(), aclList, -1, false);

                if (acquiredPermitPath != null) {
                    acquiredPermitIds.add(acquiredPermitPath);
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

    @Override
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
                acquiredPermitPath = zkReservationManager.acquireForSemaphore(ownerId, pathContext, clusterId,
                        semaphoreName, ReservationType.SEMAPHORE, permitPoolSize(), aclList, 0, false);

                if (acquiredPermitPath != null) {
                    acquiredPermitIds.add(acquiredPermitPath);
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
        return zkReservationManager.getReservationList(pathContext, clusterId, semaphoreName,
                ReservationType.SEMAPHORE, true);
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
        for (String acquiredPermitPath : acquiredPermitIds) {
            if (zkReservationManager.relinquish(acquiredPermitPath)) {
                permitIdsReleased.add(acquiredPermitPath);
                permitsReleased++;
                if (permitsReleased >= permitsToRelease) {
                    break;
                }
            }
        }

        // remove from local set
        acquiredPermitIds.removeAll(permitIdsReleased);

        // sanity check
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
            String acquiredPermitPath = zkReservationManager.acquireForSemaphore(ownerId, pathContext, clusterId,
                    semaphoreName, ReservationType.SEMAPHORE, permitPoolSize(), aclList, 0, false);

            if (acquiredPermitPath != null) {
                acquiredPermitIds.add(acquiredPermitPath);
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
                String acquiredPermitPath = zkReservationManager.acquireForSemaphore(ownerId, pathContext, clusterId,
                        semaphoreName, ReservationType.SEMAPHORE, permitPoolSize(), aclList, timeWaitMillis, true);
                if (acquiredPermitPath != null) {
                    tmpAcquiredPermitPathSet.add(acquiredPermitPath);
                }
            }

            // check that all permits were acquired; if not, return all
            if (tmpAcquiredPermitPathSet.size() < permits) {
                // return all permits acquired thus far in method
                for (String tmpAcquiredPermitPath : tmpAcquiredPermitPathSet) {
                    zkReservationManager.relinquish(tmpAcquiredPermitPath);
                }
                return false;
            }

            if (tmpAcquiredPermitPathSet.size() > 0) {
                acquiredPermitIds.addAll(tmpAcquiredPermitPathSet);
            }

            return true;
        } catch (InterruptedException e) {
            // return all permits acquired thus far in method
            for (String acquiredPermitPath : tmpAcquiredPermitPathSet) {
                zkReservationManager.relinquish(acquiredPermitPath);
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
                String acquiredPermitPath = zkReservationManager.acquireForSemaphore(ownerId, pathContext, clusterId,
                        semaphoreName, ReservationType.SEMAPHORE, permitPoolSize(), aclList, 0, true);
                if (acquiredPermitPath != null) {
                    tmpAcquiredPermitPathSet.add(acquiredPermitPath);
                } else {
                    // return all permits acquired thus far in method
                    for (String tmpAcquiredPermitPath : tmpAcquiredPermitPathSet) {
                        zkReservationManager.relinquish(tmpAcquiredPermitPath);
                    }
                    return false;
                }
            }
            if (tmpAcquiredPermitPathSet.size() > 0) {
                acquiredPermitIds.addAll(tmpAcquiredPermitPathSet);
            }

            return true;
        } catch (InterruptedException e) {
            // return all permits acquired thus far in method
            for (String acquiredPermitPath : tmpAcquiredPermitPathSet) {
                zkReservationManager.relinquish(acquiredPermitPath);
            }

            return false;
        }
    }

    public synchronized boolean tryAcquire(long wait, TimeUnit timeUnit) throws InterruptedException {
        if (!isPermitAllocationAvailable(1)) {
            return false;
        }

        long timeWaitMillis = TimeUnitUtils.toMillis(wait, timeUnit);

        String acquiredPermitPath = zkReservationManager.acquireForSemaphore(ownerId, pathContext, clusterId,
                semaphoreName, ReservationType.SEMAPHORE, permitPoolSize(), aclList, timeWaitMillis, false);

        if (acquiredPermitPath != null) {
            acquiredPermitIds.add(acquiredPermitPath);
        } else {
            return false;
        }

        return true;
    }

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
