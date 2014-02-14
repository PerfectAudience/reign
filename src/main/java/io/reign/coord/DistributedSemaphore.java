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

/**
 * Based on {@link java.util.concurrent.Semaphore}.
 * 
 * @author ypai
 * 
 */
public interface DistributedSemaphore {

    public String acquire() throws InterruptedException;

    public Collection<String> acquire(int permits) throws InterruptedException;

    public boolean isRevoked(String permitId);

    public void release(String permitId);

    public void release(Collection<String> permitIds);

    public void release();

    public void release(int permitsToRelease);

    public Collection<String> getAcquiredPermitIds();

    public int permitPoolSize();

    public int availablePermits();

    public void destroy();

    public void revoke(String permitId);

}
