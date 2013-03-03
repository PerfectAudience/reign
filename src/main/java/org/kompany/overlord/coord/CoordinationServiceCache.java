package org.kompany.overlord.coord;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

/**
 * Tracks outstanding locks, semaphores, barriers, etc.
 * 
 * @author ypai
 * 
 */
public class CoordinationServiceCache {

    private static final Logger logger = LoggerFactory.getLogger(CoordinationServiceCache.class);

    private final Multimap<String, DistributedSemaphore> semaphoreCache = Multimaps.synchronizedSetMultimap(Multimaps
            .newSetMultimap(Maps.<String, Collection<DistributedSemaphore>> newHashMap(),
                    new Supplier<HashSet<DistributedSemaphore>>() {
                        @Override
                        public HashSet<DistributedSemaphore> get() {
                            return Sets.newHashSet();
                        }
                    }));

    private final ConcurrentMap<String, PermitPoolSize> permitPoolSizeCache = new ConcurrentHashMap<String, PermitPoolSize>(
            8, 0.9f, 2);

    private final Multimap<String, DistributedLock> lockCache = Multimaps.synchronizedSetMultimap(Multimaps
            .newSetMultimap(Maps.<String, Collection<DistributedLock>> newHashMap(),
                    new Supplier<HashSet<DistributedLock>>() {
                        @Override
                        public HashSet<DistributedLock> get() {
                            return Sets.newHashSet();
                        }
                    }));

    public Collection<DistributedLock> getLocks(String clusterId, String lockName, ReservationType reservationType) {
        return lockCache.get(getKey(clusterId, lockName, reservationType));
    }

    public void putLock(String clusterId, String lockName, ReservationType reservationType, DistributedLock lock) {
        lockCache.put(getKey(clusterId, lockName, reservationType), lock);
        logger.info("lockCache.size()={}", lockCache.size());
    }

    public void removeLock(String clusterId, String lockName, ReservationType reservationType, DistributedLock lock) {
        lockCache.remove(getKey(clusterId, lockName, reservationType), lock);
        logger.info("lockCache.size()={}", lockCache.size());
    }

    public Collection<DistributedSemaphore> getSemaphores(String clusterId, String semaphoreName) {
        return semaphoreCache.get(getKey(clusterId, semaphoreName));
    }

    public void putSemaphore(String clusterId, String semaphoreName, DistributedSemaphore semaphore) {
        semaphoreCache.put(getKey(clusterId, semaphoreName), semaphore);
        logger.info("semaphoreCache.size()={}", semaphoreCache.size());

    }

    public void removeSemaphore(String clusterId, String semaphoreName, DistributedSemaphore semaphore) {
        semaphoreCache.remove(getKey(clusterId, semaphoreName), semaphore);
        logger.info("semaphoreCache.size()={}", semaphoreCache.size());
    }

    public PermitPoolSize getPermitPoolSize(String clusterId, String semaphoreName) {
        return permitPoolSizeCache.get(getKey(clusterId, semaphoreName));
    }

    public PermitPoolSize putOrReturnCachedPermitPoolSize(String clusterId, String semaphoreName, PermitPoolSize pps) {
        PermitPoolSize value = permitPoolSizeCache.putIfAbsent(getKey(clusterId, semaphoreName), pps);
        if (value == null) {
            value = pps;
        }
        logger.info("permitPoolSizeCache.size()={}", permitPoolSizeCache.size());
        return value;

    }

    public void removePermitPoolSize(String clusterId, String semaphoreName, PermitPoolSize pps) {
        permitPoolSizeCache.remove(getKey(clusterId, semaphoreName));
        logger.info("permitPoolSizeCache.size()={}", permitPoolSizeCache.size());
    }

    String getKey(String clusterId, String lockName) {
        return clusterId + "/" + lockName;
    }

    String getKey(String clusterId, String lockName, ReservationType reservationType) {
        return clusterId + "/" + lockName + "/" + reservationType.prefix();
    }
}
