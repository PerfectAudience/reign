package org.kompany.sovereign.coord;

import java.util.concurrent.locks.Lock;

/**
 * 
 * @author ypai
 * 
 */
public interface DistributedLock extends Lock {

    public String getLockId();

    public boolean isRevoked();

    public void revoke(String reservationId);

    public void destroy();

}
