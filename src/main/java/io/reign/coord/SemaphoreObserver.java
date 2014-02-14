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
public abstract class SemaphoreObserver extends CoordObserver<DistributedSemaphore> {
    /**
     * @param semaphore
     * @param permitId
     *            the acquired permit that was revoked.
     */
    public abstract void revoked(DistributedSemaphore semaphore, String permitId);

    @Override
    public void nodeChildrenChanged(List<String> updatedChildList, List<String> previousChildList) {
        // figure out revoked permit
        List<String> revoked = findRevoked(updatedChildList, previousChildList, getPath(), pathScheme);

        // check and signal revocations to observers
        if (revoked.size() > 0) {
            Collection<DistributedSemaphore> semaphores = this.coordinationServiceCache.getSemaphores(getPath());
            for (DistributedSemaphore semaphore : semaphores) {
                // check and see which ones have been revoked
                for (String revokedId : revoked) {
                    if (semaphore.getAcquiredPermitIds().contains(revokedId)) {
                        semaphore.revoke(revokedId);
                        revoked(semaphore, revokedId);
                    }// if
                }// for
            }// for
        }// if

    }// if

}
