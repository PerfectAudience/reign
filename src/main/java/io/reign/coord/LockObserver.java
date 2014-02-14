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

import java.util.Collection;
import java.util.List;

/**
 * 
 * @author ypai
 * 
 */
public abstract class LockObserver extends CoordObserver<DistributedLock> {

    /**
     * @param lock
     *            the acquired lock that was revoked.
     */
    public abstract void revoked(DistributedLock lock, String reservationId);

    @Override
    public void nodeChildrenChanged(List<String> updatedChildList, List<String> previousChildList) {
        // figure out revoked permit
        List<String> revoked = findRevoked(updatedChildList, previousChildList, getPath(), pathScheme);

        // check and signal revocations to observers
        if (revoked.size() > 0) {
            for (String revokedId : revoked) {
                signalIfRevoked(revokedId, ReservationType.LOCK_EXCLUSIVE);
                signalIfRevoked(revokedId, ReservationType.LOCK_SHARED);
            }
        }// if

    }// if

    void signalIfRevoked(String revokedId, ReservationType reservationType) {
        Collection<DistributedLock> locks = this.coordinationServiceCache.getLocks(getPath(), reservationType);
        for (DistributedLock lock : locks) {
            if (revokedId.equals(lock.getReservationId())) {
                lock.revoke(revokedId);
                revoked(lock, revokedId);
            }
        }
    }
}
