package org.kompany.overlord.coord;


/**
 * 
 * @author ypai
 * 
 */
public interface DistributedReentrantLock extends DistributedLock {

    public int getHoldCount();

}
