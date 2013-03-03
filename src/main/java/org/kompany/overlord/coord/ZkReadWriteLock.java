package org.kompany.overlord.coord;

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
