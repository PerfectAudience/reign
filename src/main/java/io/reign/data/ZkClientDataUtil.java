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
import io.reign.ZkClient;
import io.reign.coord.DistributedReadWriteLock;
import io.reign.util.PathCache;
import io.reign.util.ZkClientUtil;

import java.util.Map;

import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ypai
 * 
 */
public class ZkClientDataUtil extends ZkClientUtil {

    private static final Logger logger = LoggerFactory.getLogger(ZkClientDataUtil.class);

    protected final ZkClient zkClient;
    protected final PathScheme pathScheme;
    protected final PathCache pathCache;
    protected final TranscodingScheme transcodingScheme;

    ZkClientDataUtil(ZkClient zkClient, PathScheme pathScheme, PathCache pathCache, TranscodingScheme transcodingScheme) {

        this.zkClient = zkClient;
        this.pathScheme = pathScheme;
        this.pathCache = pathCache;
        this.transcodingScheme = transcodingScheme;

    }

    void lockForWrite(DistributedReadWriteLock readWriteLock) {
        if (readWriteLock != null) {
            readWriteLock.writeLock().lock();
        }// if
    }

    void lockForWrite(DistributedReadWriteLock readWriteLock, String dataPath, final Object monitorObject) {
        if (readWriteLock != null) {
            readWriteLock.writeLock().lock();
            syncZkClient(dataPath, monitorObject);
        }// if
    }

    void unlockForWrite(DistributedReadWriteLock readWriteLock) {
        if (readWriteLock != null) {
            readWriteLock.writeLock().unlock();
        }
    }

    void lockForRead(DistributedReadWriteLock readWriteLock, String dataPath, final Object monitorObject) {
        if (readWriteLock != null) {
            readWriteLock.readLock().lock();
            syncZkClient(dataPath, monitorObject);
        }
    }

    void unlockForRead(DistributedReadWriteLock readWriteLock) {
        if (readWriteLock != null) {
            readWriteLock.readLock().unlock();
        }
    }

    void syncZkClient(String dataPath, final Object monitorObject) {
        logger.trace("Syncing ZK client:  dataPath={}", dataPath);
        zkClient.sync(dataPath, new VoidCallback() {
            @Override
            public void processResult(int arg0, String arg1, Object arg2) {
                synchronized (monitorObject) {
                    monitorObject.notifyAll();
                }
            }
        }, null);

        // wait for sync to complete
        synchronized (monitorObject) {
            try {
                logger.trace("Waiting for ZK client sync complete:  dataPath={}", dataPath);
                monitorObject.wait();
                logger.trace("ZK client sync completed:  dataPath={}", dataPath);
            } catch (InterruptedException e) {
                logger.warn("Interrupted while waiting for ZK sync():  " + e, e);
            }
        }
    }

    boolean isExpired(long lastModifiedMillis, int ttlMillis) {
        return ttlMillis > 0 && lastModifiedMillis + ttlMillis < System.currentTimeMillis();
    }

}
