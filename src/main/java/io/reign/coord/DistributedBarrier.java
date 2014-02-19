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

import java.util.concurrent.TimeUnit;

/**
 * Based on {@link java.util.concurrent.CyclicBarrier}.
 * 
 * @author ypai
 * 
 */
public interface DistributedBarrier {

    /**
     * 
     * @return number of parties required to trip this barrier.
     */
    public int getParties();

    /**
     * Block until all parties arrive at barrier.
     * 
     * @return 0 if last to arrive; getParties()-1 if first to arrive
     */
    public int await();

    public int await(long timeout, TimeUnit timeUnit);

    /**
     * 
     * @return true if a party broke through or dropped out.
     */
    public boolean isBroken();

    /**
     * 
     */
    public void reset();

    public int getNumberWaiting();

    public void destroy();

}
