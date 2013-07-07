package io.reign.data;

import io.reign.PathScheme;
import io.reign.ZkClient;
import io.reign.coord.DistributedReadWriteLock;
import io.reign.util.PathCache;

import java.util.List;

/**
 * 
 * @author ypai
 * 
 * @param <K>
 * @param <V>
 */
public class ZkMultiMapData<K, V> implements MultiMapData<K, V> {

    private final DistributedReadWriteLock readWriteLock;

    private final String relativeBasePath;

    private final ZkClient zkClient;
    private final PathCache pathCache;

    private final PathScheme pathScheme;

    /**
     * 
     * @param basePath
     * @param readWriteLock
     *            for inter-process safety; can be null
     */
    public ZkMultiMapData(String relativeBasePath, PathScheme pathScheme, DistributedReadWriteLock readWriteLock,
            ZkClient zkClient, PathCache pathCache) {
        this.relativeBasePath = relativeBasePath;
        this.pathScheme = pathScheme;
        this.readWriteLock = readWriteLock;
        this.zkClient = zkClient;
        this.pathCache = pathCache;
    }

    @Override
    public synchronized void destroy() {
        if (readWriteLock != null) {
            readWriteLock.destroy();
        }
    }

    @Override
    public synchronized void put(K key, String index, V value) {
        lockForWrite();
        try {

        } finally {
            unlockForWrite();
        }
        // TODO Auto-generated method stub

    }

    @Override
    public synchronized void put(K key, V value) {
        lockForWrite();
        try {

        } finally {
            unlockForWrite();
        }
        // TODO Auto-generated method stub

    }

    @Override
    public synchronized <T extends V> T get(K key) {
        lockForRead();
        try {

        } finally {
            unlockForRead();
        }
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized <T extends V> T get(K key, String index) {
        lockForRead();
        try {

        } finally {
            unlockForRead();
        }
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized <T extends List> T getAll(K key) {
        lockForRead();
        try {

        } finally {
            unlockForRead();
        }
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized String remove(K key) {
        lockForWrite();
        try {

        } finally {
            unlockForWrite();
        }
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized String remove(K key, String index) {
        lockForWrite();
        try {

        } finally {
            unlockForWrite();
        }
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized List<String> removeAll(K key) {
        lockForWrite();
        try {

        } finally {
            unlockForWrite();
        }
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized int size() {
        lockForRead();
        try {

        } finally {
            unlockForRead();
        }
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public synchronized List<String> keys() {
        lockForRead();
        try {

        } finally {
            unlockForRead();
        }
        // TODO Auto-generated method stub
        return null;
    }

    void lockForWrite() {
        if (readWriteLock != null) {
            readWriteLock.writeLock().lock();
        }
    }

    void unlockForWrite() {
        if (readWriteLock != null) {
            readWriteLock.writeLock().unlock();
        }
    }

    void lockForRead() {
        if (readWriteLock != null) {
            readWriteLock.readLock().lock();
        }
    }

    void unlockForRead() {
        if (readWriteLock != null) {
            readWriteLock.readLock().unlock();
        }
    }
}
