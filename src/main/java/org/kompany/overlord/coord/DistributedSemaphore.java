package org.kompany.overlord.coord;

/**
 * 
 * @author ypai
 * 
 */
public interface DistributedSemaphore {

    public void acquire() throws InterruptedException;

    public void acquire(int permits) throws InterruptedException;

    public void release();

    public void release(int permitsToRelease);

    public int permitPoolSize();
}
