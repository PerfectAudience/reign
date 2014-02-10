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

package io.reign;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for managing observers for services. Deals with multiple observers for a single path, etc.
 * 
 * @author ypai
 * 
 * @param <T>
 *            the type of observer wrapper
 */
public class NodeObserverManager<T extends NodeObserver> extends AbstractZkEventHandler {

    private static final Logger logger = LoggerFactory.getLogger(NodeObserverManager.class);

    private final ConcurrentMap<String, Set<T>> observerMap = new ConcurrentHashMap<String, Set<T>>(16, 0.9f, 2);

    private ZkClient zkClient;

    public NodeObserverManager() {
    }

    public ZkClient getZkClient() {
        return zkClient;
    }

    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    public void init() {
        this.zkClient.register(this);
    }

    public void put(String path, T observer) {
        try {
            // decorate observer with data about the path so we can handle notifications correctly
            byte[] data = zkClient.getData(path, true, new Stat());
            List<String> childList = zkClient.getChildren(path, true);

            observer.setPath(path);
            observer.setData(data);
            observer.setChildList(childList);

            Set<T> observerSet = getObserverSet(path, true);
            observerSet.add(observer);

            logger.info("Added observer:  path={}; pathObserverCount={}", path, observerSet.size());
        } catch (KeeperException e) {
            if (e.code() == Code.NONODE) {
                // set up watch on that node
                try {
                    zkClient.exists(path, true);

                    observer.setPath(path);
                    observer.setChildList(Collections.EMPTY_LIST);
                    Set<T> observerSet = getObserverSet(path, true);
                    observerSet.add(observer);

                } catch (Exception e1) {
                    logger.error("Unable to add observer:  path=" + path + "; observerType="
                            + observer.getClass().getSimpleName(), e);
                }

            } else {
                logger.error("Unable to add observer:  path=" + path + "; observerType="
                        + observer.getClass().getSimpleName(), e);
            }
        } catch (Exception e) {
            logger.error("Unable to add observer:  path=" + path + "; observerType="
                    + observer.getClass().getSimpleName(), e);
        }

    }

    // public Set<T> getObserverWrapperSet(String path) {
    // Set<T> observerSet = getObserverSet(path, false);
    // return Collections.unmodifiableSet(observerSet);
    // }

    @Override
    public boolean filterWatchedEvent(WatchedEvent event) {
        if (this.getObserverSet(event.getPath(), false) == null) {
            // ignore events that are not being tracked by an observer
            return true;
        }
        return false;
    }

    @Override
    public void nodeChildrenChanged(WatchedEvent event) {
        logger.info("Notifying ALL observers:  nodeChildrenChanged");

        String path = event.getPath();
        try {
            Set<T> observerSet = getObserverSet(path, false);
            if (observerSet.size() > 0) {
                List<String> updatedChildList = zkClient.getChildren(path, true);
                if (updatedChildList == null) {
                    updatedChildList = Collections.EMPTY_LIST;
                }

                for (T observer : observerSet) {
                    List<String> childList = observer.getChildList();
                    if (childList == null) {
                        childList = Collections.EMPTY_LIST;
                    }

                    boolean updatedValueDiffers = childList.size() != updatedChildList.size();

                    // if sizes are the same, compare contents independent of order
                    if (!updatedValueDiffers) {
                        Set<String> childListSet = new HashSet<String>(childList.size() + 1, 1.0f);
                        childListSet.addAll(childList);

                        Set<String> updatedChildListSet = new HashSet<String>(updatedChildList.size() + 1, 1.0f);
                        updatedChildListSet.addAll(updatedChildList);

                        updatedValueDiffers = childListSet.equals(updatedChildListSet);
                    }

                    if (updatedValueDiffers) {
                        observer.nodeChildrenChanged(updatedChildList);
                    }
                }
            }
        } catch (KeeperException e) {
            logger.warn("Unable to notify observers:  path=" + path, e);
        } catch (Exception e) {
            logger.warn("Unable to notify observers:  path=" + path, e);
        }
    }

    @Override
    public void nodeCreated(WatchedEvent event) {
        logger.info("Notifying ALL observers:  nodeCreated");

        String path = event.getPath();
        try {
            Set<T> observerSet = getObserverSet(path, false);
            if (observerSet.size() > 0) {
                byte[] data = zkClient.getData(path, true, new Stat());
                for (T observer : observerSet) {
                    if (Arrays.equals(observer.getData(), data)) {
                        observer.nodeCreated(data);
                    }
                }
            }
        } catch (KeeperException e) {
            logger.warn("Unable to notify observers:  path=" + path, e);
        } catch (Exception e) {
            logger.warn("Unable to notify observers:  path=" + path, e);
        }
    }

    @Override
    public void nodeDataChanged(WatchedEvent event) {
        logger.info("Notifying ALL observers:  nodeDataChanged");

        String path = event.getPath();
        try {
            Set<T> observerSet = getObserverSet(path, false);
            if (observerSet.size() > 0) {
                byte[] updatedData = zkClient.getData(path, true, new Stat());
                for (T observer : observerSet) {
                    if (Arrays.equals(observer.getData(), updatedData)) {
                        observer.nodeDataChanged(updatedData);
                    }
                }
            }
        } catch (KeeperException e) {
            logger.warn("Unable to notify observers:  path=" + path, e);
        } catch (Exception e) {
            logger.warn("Unable to notify observers:  path=" + path, e);
        }
    }

    @Override
    public void nodeDeleted(WatchedEvent event) {
        logger.info("Notifying ALL observers:  nodeDeleted");

        String path = event.getPath();

        Set<T> observerSet = getObserverSet(path, false);
        if (observerSet.size() > 0) {
            for (T observer : observerSet) {
                observer.nodeDeleted();
            }
        }
    }

    public void removeAll(String path) {
        logger.info("Removing ALL observers:  path={}", path);

        observerMap.remove(path);
    }

    public void signalStateReset(Object o) {
        logger.info("Notifying ALL observers:  signalStateReset");
        for (String path : this.observerMap.keySet()) {
            Set<T> observerSet = getObserverSet(path, false);
            for (T observer : observerSet) {
                observer.stateReset(o);
            }
        }
    }

    public void signalStateUnknown(Object o) {
        logger.info("Notifying ALL observers:  signalStateUnknown");
        for (String path : this.observerMap.keySet()) {
            Set<T> observerSet = getObserverSet(path, false);
            for (T observer : observerSet) {
                observer.stateUnknown(o);
            }
        }
    }

    // public void signal(String path, Object o) {
    // Set<T> wrapperSet = getObserverSet(path, false);
    //
    // logger.info("Notifying observers:  path={}; pathObserverCount={}", path, wrapperSet.size());
    //
    // for (T wrapper : wrapperSet) {
    // wrapper.signalObserver(o);
    // }
    // }

    public boolean isBeingObserved(String path) {
        Set<T> wrapperSet = getObserverSet(path, false);
        return wrapperSet.size() > 0;

    }

    Set<T> getObserverSet(String path, boolean createIfNecessary) {
        Set<T> observerSet = observerMap.get(path);

        if (observerSet == null) {
            if (createIfNecessary) {
                Set<T> newObserverSet = Collections.newSetFromMap(new ConcurrentHashMap<T, Boolean>(4, 0.9f, 2));
                observerSet = observerMap.putIfAbsent(path, newObserverSet);
                if (observerSet == null) {
                    observerSet = newObserverSet;
                }
            } else {
                observerSet = Collections.EMPTY_SET;
            }
        }

        return observerSet;
    }
}
