package io.reign.coord;


/**
 * 
 * @author ypai
 * 
 */
public interface DistributedReentrantLock extends DistributedLock {

    public int getHoldCount();

}
