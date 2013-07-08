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
