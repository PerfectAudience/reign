package io.reign.data;

import io.reign.PathScheme;
import io.reign.ZkClient;
import io.reign.coord.DistributedReadWriteLock;

import java.util.List;

/**
 * 
 * @author ypai
 * 
 * @param <V>
 */
public class ZkMultiData<V> implements MultiData<V> {

    private final DistributedReadWriteLock readWriteLock;

    private final String relativeBasePath;

    private final PathScheme pathScheme;

    private final ZkClient zkClient;

    private final int ttlMillis;

    /**
     * 
     * @param relativeBasePath
     * @param readWriteLock
     *            for inter-process safety; can be null
     */
    public ZkMultiData(String relativeBasePath, PathScheme pathScheme, DistributedReadWriteLock readWriteLock,
            ZkClient zkClient, int ttlMillis) {
        this.relativeBasePath = relativeBasePath;
        this.pathScheme = pathScheme;
        this.readWriteLock = readWriteLock;
        this.zkClient = zkClient;
        this.ttlMillis = ttlMillis;
    }

    @Override
    public synchronized void set(V value) {
        lockForWrite();
        try {

        } finally {
            unlockForWrite();
        }
        // TODO Auto-generated method stub

    }

    @Override
    public synchronized void set(String index, V value) {
        lockForWrite();
        try {

        } finally {
            unlockForWrite();
        }
        // TODO Auto-generated method stub

    }

    @Override
    public synchronized V get() {
        lockForRead();
        try {

        } finally {
            unlockForRead();
        }
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized List<V> getAll() {
        lockForRead();
        try {

        } finally {
            unlockForRead();
        }
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized String remove() {
        lockForWrite();
        try {

        } finally {
            unlockForWrite();
        }
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized List<String> removeAll() {
        lockForWrite();
        try {

        } finally {
            unlockForWrite();
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
