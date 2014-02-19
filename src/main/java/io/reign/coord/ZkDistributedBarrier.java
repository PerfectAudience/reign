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

import io.reign.AbstractObserver;
import io.reign.ReignContext;
import io.reign.util.TimeUnitUtil;
import io.reign.util.ZkClientUtil;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class ZkDistributedBarrier implements DistributedBarrier {

    private static final Logger logger = LoggerFactory.getLogger(ZkDistributedBarrier.class);

    private volatile int parties;

    private volatile boolean broken = false;

    private volatile boolean conditionsMet = false;

    private final String entityPath;

    private final String ownerId;

    private final ReignContext context;

    private final ZkClientUtil zkClientUtil = new ZkClientUtil();

    private final AbstractObserver observer = new AbstractObserver() {
        @Override
        public void nodeChildrenChanged(List<String> updatedChildList, List<String> previousChildList) {
            if (!conditionsMet) {
                Set<String> updatedChildSet = new HashSet<String>(updatedChildList.size() + 1, 1.0f);
                for (String child : updatedChildList) {
                    updatedChildSet.add(child);
                }
                for (String child : previousChildList) {
                    if (!updatedChildSet.contains(child)) {
                        logger.warn("Barrier is broken:  old child not in new update:  child={}; updatedChildList={}",
                                child, updatedChildList);
                        broken = true;
                        break;
                    }
                }
                if (!broken && updatedChildList.size() == parties) {
                    conditionsMet = true;
                }

            }// if

            if (broken || conditionsMet) {
                synchronized (ZkDistributedBarrier.this) {
                    ZkDistributedBarrier.this.notifyAll();
                }
            }

            logger.trace("observer.nodeChildrenChanged():  broken={}; conditionsMet={}", broken, conditionsMet);
        }

        @Override
        public void nodeDeleted(byte[] previousData, List<String> previousChildList) {
            broken = false;
            conditionsMet = false;
        }
    };

    public ZkDistributedBarrier(String entityPath, String ownerId, int parties, ReignContext context) {
        this.entityPath = entityPath;
        this.ownerId = ownerId;
        this.parties = parties;
        this.context = context;

    }

    @Override
    public int getParties() {
        return parties;
    }

    @Override
    public synchronized int await() {
        return await(-1, TimeUnit.SECONDS);
    }

    @Override
    public synchronized int await(long timeout, TimeUnit timeUnit) {
        if (conditionsMet) {
            throw new IllegalStateException("Barrier conditions have been met:  call reset() to re-use barrier.");
        }

        try {
            // -1 means wait forever

            // owner data in JSON
            String lockReservationData = "{\"ownerId\":\"" + ownerId + "\"}";

            // path to lock reservation node (to "get in line" for lock)
            String lockReservationPrefix = CoordServicePathUtil.getAbsolutePathReservationPrefix(
                    context.getPathScheme(), entityPath, ReservationType.BARRIER);

            // add observer at entity path
            context.getObserverManager().put(entityPath, observer);

            // create reservation sequential node
            String lockReservationPath = zkClientUtil.updatePath(context.getZkClient(), context.getPathScheme(),
                    lockReservationPrefix, lockReservationData.getBytes("UTF-8"), context.getDefaultZkAclList(),
                    CreateMode.EPHEMERAL_SEQUENTIAL, -1);

            List<String> childList = getChildList();
            int index = parties - childList.size();

            if (index > 0) {
                synchronized (this) {
                    if (timeout == -1) {
                        wait();
                    } else {
                        wait(TimeUnitUtil.toMillis(timeout, timeUnit));
                    }
                }

                // see if we got out of wait without meeting barrier conditions
                childList = getChildList();
                if (childList.size() != parties) {
                    logger.warn("Barrier is broken:  childList.size() != parties: {} != {}", childList.size(), parties);
                    broken = true;
                }
            }

            throwExceptionIfBroken();

            // return parties - child list size
            return index;
        } catch (Exception e) {
            throw new IllegalStateException("Error while waiting at barrier:  " + e, e);
        }
    }

    @Override
    public boolean isBroken() {
        return broken || !conditionsMet;
    }

    @Override
    public synchronized void reset() {
        // delete all barrier nodes
        List<String> childList = getChildList();
        for (String child : childList) {
            String childPath = context.getPathScheme().joinPaths(entityPath, child);
            try {
                context.getZkClient().delete(childPath, -1);
            } catch (KeeperException e) {
                if (e.code() != KeeperException.Code.NONODE) {
                    throw new IllegalStateException(e);
                } else {
                    logger.trace("Already deleted ZK barrier node:  " + e + "; path=" + childPath, e);
                }
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        // delete entity node to reset broken state
        try {
            context.getZkClient().delete(entityPath, -1);
        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NONODE) {
                throw new IllegalStateException(e);
            } else {
                logger.trace("Already deleted ZK barrier node:  " + e + "; path=" + entityPath, e);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

    }

    @Override
    public synchronized int getNumberWaiting() {
        return getChildList().size();
    }

    @Override
    public void destroy() {
        context.getObserverManager().remove(entityPath, observer);
    }

    private void throwExceptionIfBroken() {
        if (broken) {
            throw new IllegalStateException("Barrier has been broken!");
        }
    }

    private List<String> getChildList() {
        List<String> childList = Collections.EMPTY_LIST;
        try {
            childList = context.getZkClient().getChildren(entityPath, true);
        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NONODE) {
                throw new IllegalStateException(e);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return childList;
    }

}
