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

package io.reign.coord;

import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class ZkReadWriteLock implements DistributedReadWriteLock {

    private static final Logger logger = LoggerFactory.getLogger(ZkReadWriteLock.class);

    private final DistributedLock readLock;
    private final DistributedLock writeLock;

    public ZkReadWriteLock(DistributedLock readLock, DistributedLock writeLock) {
        super();
        this.readLock = readLock;
        this.writeLock = writeLock;
    }

    @Override
    public Lock readLock() {
        return readLock;
    }

    @Override
    public Lock writeLock() {
        return writeLock;
    }

    @Override
    public void destroy() {
        logger.info("destroy() called");
        readLock.destroy();
        writeLock.destroy();
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        destroy();
    }

}
