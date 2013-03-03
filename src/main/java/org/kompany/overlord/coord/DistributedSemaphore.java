package org.kompany.overlord.coord;

import java.util.Collection;

/**
 * 
 * @author ypai
 * 
 */
public interface DistributedSemaphore {

    public String acquire() throws InterruptedException;

    public Collection<String> acquire(int permits) throws InterruptedException;

    public boolean isValid(String permitId);

    public void release(String permitId);

    public void release(Collection<String> permitIds);

    public void release();

    public void release(int permitsToRelease);

    public int permitPoolSize();

    public int availablePermits();
}
