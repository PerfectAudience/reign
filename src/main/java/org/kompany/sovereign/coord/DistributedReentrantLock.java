package org.kompany.sovereign.coord;


/**
 * 
 * @author ypai
 * 
 */
public interface DistributedReentrantLock extends DistributedLock {

    public int getHoldCount();

}
