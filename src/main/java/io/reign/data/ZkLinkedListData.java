/*
 Copyright 2013 Yen Pai ypai@reign.io

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package io.reign.data;

import io.reign.PathScheme;
import io.reign.ZkClient;
import io.reign.coord.DistributedReadWriteLock;
import io.reign.util.PathCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base implementation is thread-safe within the same JVM. Construct with read/write lock for inter-process safety.
 * 
 * @author ypai
 * 
 * @param <V>
 */
public class ZkLinkedListData<V> implements LinkedListData<V> {

    private static final Logger logger = LoggerFactory.getLogger(ZkLinkedListData.class);

    private final DistributedReadWriteLock readWriteLock;

    private final int maxSize;

    private final String relativeBasePath;

    private final PathScheme pathScheme;

    private final ZkClient zkClient;
    private final PathCache pathCache;

    private final int ttlMillis;

    /**
     * @param maxSize
     * @param basePath
     * @param readWriteLock
     *            for inter-process safety; can be null
     */
    public ZkLinkedListData(int maxSize, String relativeBasePath, PathScheme pathScheme,
            DistributedReadWriteLock readWriteLock, ZkClient zkClient, PathCache pathCache, int ttlMillis) {
        this.maxSize = maxSize;
        this.relativeBasePath = relativeBasePath;
        this.pathScheme = pathScheme;
        this.readWriteLock = readWriteLock;
        this.zkClient = zkClient;
        this.pathCache = pathCache;
        this.ttlMillis = ttlMillis;
    }

    @Override
    public synchronized void destroy() {
        if (readWriteLock != null) {
            readWriteLock.destroy();
        }
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
