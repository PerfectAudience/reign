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

import io.reign.DataSerializer;
import io.reign.PathScheme;
import io.reign.ReignContext;
import io.reign.ZkClient;
import io.reign.coord.DistributedReadWriteLock;

import java.util.List;
import java.util.Map;

import org.apache.zookeeper.data.ACL;
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

    private final String absoluteBasePath;

    private final PathScheme pathScheme;
    private final ZkClient zkClient;
    // private final PathCache pathCache;

    private final List<ACL> aclList;

    private final ZkClientLinkedListDataUtil zkClientLinkedListDataUtil;

    /**
     * @param maxSize
     * @param basePath
     * @param readWriteLock
     *            for inter-process safety; cannot be null
     */
    public ZkLinkedListData(String absoluteBasePath, DistributedReadWriteLock readWriteLock, List<ACL> aclList,
            TranscodingScheme transcodingScheme, ReignContext context) {
        if (readWriteLock == null) {
            throw new IllegalArgumentException("readWriteLock must not be null!");
        }

        this.absoluteBasePath = absoluteBasePath;
        this.pathScheme = context.getPathScheme();
        this.readWriteLock = readWriteLock;
        this.zkClient = context.getZkClient();
        // this.pathCache = context.getPathCache();
        this.aclList = aclList;

        this.zkClientLinkedListDataUtil = new ZkClientLinkedListDataUtil(context.getZkClient(),
                context.getPathScheme(), transcodingScheme);
    }

    @Override
    public synchronized void destroy() {
        if (readWriteLock != null) {
            readWriteLock.destroy();
        }
    }

    @Override
    public synchronized <T extends V> T popAt(int index, Class<T> typeClass) {
        return getValueAt(index, typeClass, true);
    }

    @Override
    public synchronized <T extends V> T peekAt(int index, Class<T> typeClass) {
        return getValueAt(index, typeClass, false);
    }

    @Override
    public synchronized <T extends V> T popFirst(Class<T> typeClass) {
        return getValueAt(0, typeClass, true);
    }

    @Override
    public synchronized <T extends V> T popLast(Class<T> typeClass) {
        return getValueAt(-1, typeClass, true);
    }

    @Override
    public synchronized <T extends V> T peekFirst(Class<T> typeClass) {
        return getValueAt(0, typeClass, false);
    }

    @Override
    public synchronized <T extends V> T peekLast(Class<T> typeClass) {
        return getValueAt(-1, typeClass, false);
    }

    @Override
    public synchronized boolean pushLast(V value) {
        zkClientLinkedListDataUtil.lockForWrite(readWriteLock);
        try {
            zkClientLinkedListDataUtil.writeData(absoluteBasePath, value, aclList);
        } finally {
            zkClientLinkedListDataUtil.unlockForWrite(readWriteLock);
        }
        return false;
    }

    @Override
    public synchronized int size() {
        zkClientLinkedListDataUtil.lockForRead(readWriteLock, absoluteBasePath, this);
        try {
            return zkClientLinkedListDataUtil.getSortedChildList(absoluteBasePath).size();
        } finally {
            zkClientLinkedListDataUtil.unlockForRead(readWriteLock);
        }

    }

    /**
     * 
     * @param index
     *            if negative, designates reading from the end: -1 meaning last element, -2 second from last, etc.
     * @param typeClass
     * @param deleteAfterRead
     * @return
     */
    synchronized <T extends V> T getValueAt(int index, Class<T> typeClass, boolean deleteAfterRead) {
        if (deleteAfterRead) {
            zkClientLinkedListDataUtil.lockForWrite(readWriteLock, absoluteBasePath, this);
        } else {
            zkClientLinkedListDataUtil.lockForRead(readWriteLock, absoluteBasePath, this);
        }
        try {
            List<String> childList = zkClientLinkedListDataUtil.getSortedChildList(absoluteBasePath);
            if (index < 0) {
                index = childList.size() + index;
            }
            if (index >= 0 && childList.size() > index) {
                // get child node to read
                String child = childList.get(index);
                String absoluteDataPath = pathScheme.joinPaths(absoluteBasePath, child);

                // sync connection on path before read
                zkClientLinkedListDataUtil.syncZkClient(absoluteDataPath, this);

                // read child node value
                T childData = zkClientLinkedListDataUtil.readData(absoluteDataPath, -1, typeClass);

                // delete node
                if (deleteAfterRead) {
                    zkClientLinkedListDataUtil.deleteData(absoluteDataPath, -1);
                }

                // return value
                return childData;
            } else {
                throw new IndexOutOfBoundsException("index < 0 || index >= size():  index=" + index + "; size="
                        + childList.size());
            }
        } finally {
            if (deleteAfterRead) {
                zkClientLinkedListDataUtil.unlockForWrite(readWriteLock);
            } else {
                zkClientLinkedListDataUtil.unlockForRead(readWriteLock);
            }
        }
    }
}
