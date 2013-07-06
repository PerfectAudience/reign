package io.reign.data;

import io.reign.ZkClient;
import io.reign.coord.DistributedReadWriteLock;

/**
 * Base implementation is thread-safe within the same JVM. Construct with read/write lock for inter-process safety.
 * 
 * @author ypai
 * 
 * @param <V>
 */
public class ZkLinkedListData<V> implements LinkedListData<V> {

    private final DistributedReadWriteLock readWriteLock;

    private final int maxSize;

    private final String basePath;

    private final ZkClient zkClient;

    /**
     * @param maxSize
     * @param basePath
     * @param readWriteLock
     *            for inter-process safety; can be null
     */
    public ZkLinkedListData(int maxSize, String basePath, DistributedReadWriteLock readWriteLock, ZkClient zkClient) {
        this.maxSize = maxSize;
        this.basePath = basePath;
        this.readWriteLock = readWriteLock;
        this.zkClient = zkClient;
    }

    @Override
    public synchronized <T extends V> T popAt(int index) {
        lockForWrite();
        try {

        } finally {
            unlockForWrite();
        }
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized <T extends V> T peekAt(int index) {
        lockForRead();
        try {

        } finally {
            unlockForRead();
        }
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized <T extends V> T popFirst() {
        lockForWrite();
        try {

        } finally {
            unlockForWrite();
        }
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized <T extends V> T popLast() {
        lockForWrite();
        try {

        } finally {
            unlockForWrite();
        }
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized <T extends V> T peekFirst() {
        lockForRead();
        try {

        } finally {
            unlockForRead();
        }
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized <T extends V> T peekLast() {
        lockForRead();
        try {

        } finally {
            unlockForRead();
        }
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized boolean pushLast(V value) {
        lockForWrite();
        try {

        } finally {
            unlockForWrite();
        }
        // TODO Auto-generated method stub
        return false;
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
    public synchronized int maxSize() {
        return maxSize;
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
