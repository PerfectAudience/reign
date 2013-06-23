package io.reign.coord;

import java.util.concurrent.locks.ReadWriteLock;

/**
 * 
 * @author ypai
 * 
 */
public interface DistributedReadWriteLock extends ReadWriteLock {

    public void destroy();
}
