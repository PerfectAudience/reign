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
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for managing observers for services. Deals with multiple observers for a single path, etc.
 * 
 * @author ypai
 * 
 * @param <T>
 *            the type of observer
 */
public class ObserverManager<T extends Observer> extends AbstractZkEventHandler {

    private static final Logger logger = LoggerFactory.getLogger(ObserverManager.class);

    private final ConcurrentMap<String, Set<T>> observerMap = new ConcurrentHashMap<String, Set<T>>(16, 0.9f, 2);

    private ZkClient zkClient;

    public ObserverManager() {
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

            logger.info("Added observer:  observer.hashCode()={}; path={}; pathObserverCount={}", new Object[] {
                    observer.hashCode(), path, observerSet.size() });
        } catch (KeeperException e) {
            if (e.code() == Code.NONODE) {
                // set up watch on that node
                try {
                    zkClient.exists(path, true);

                    observer.setPath(path);
                    observer.setData(null);
                    observer.setChildList(Collections.EMPTY_LIST);

                    Set<T> observerSet = getObserverSet(path, true);
                    observerSet.add(observer);

                    logger.info("Added observer for nonexistent path:  path={}; pathObserverCount={}", path,
                            observerSet.size());
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

    @Override
    public boolean filterWatchedEvent(WatchedEvent event) {
        if (this.getObserverSet(event.getPath(), false).size() == 0) {
            // ignore events that are not being tracked by an observer
            return true;
        }
        return false;
    }

    @Override
    public void nodeChildrenChanged(WatchedEvent event) {
        String path = event.getPath();
        logger.debug("Notifying ALL observers:  nodeChildrenChanged:  path={}", path);
        try {
            Set<T> observerSet = getObserverSet(path, false);
            if (observerSet.size() > 0) {
                List<String> updatedChildList = null;
                try {
                    updatedChildList = zkClient.getChildren(path, true);
                } catch (KeeperException e) {
                    if (e.code() != KeeperException.Code.NONODE) {
                        throw e;
                    }
                }
                if (updatedChildList == null) {
                    updatedChildList = Collections.EMPTY_LIST;
                }

                for (T observer : observerSet) {

                    logger.trace("Notifying observer:  observer.hashCode()={}", observer.hashCode());

                    synchronized (observer) {
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
                            List<String> previousChildList = observer.getChildList();
                            observer.setChildList(updatedChildList);
                            observer.nodeChildrenChanged(updatedChildList, previousChildList);
                        }
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
        String path = event.getPath();
        logger.debug("Notifying ALL observers:  nodeCreated:  path={}", path);
        try {
            Set<T> observerSet = getObserverSet(path, false);
            if (observerSet.size() > 0) {
                // get children just to get a child watch
                zkClient.getChildren(path, true);

                byte[] data = zkClient.getData(path, true, new Stat());
                for (T observer : observerSet) {
                    synchronized (observer) {
                        if (!Arrays.equals(observer.getData(), data)) {
                            byte[] previousData = observer.getData();
                            observer.setData(data);
                            observer.nodeCreated(data, previousData);
                        }
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
        String path = event.getPath();
        logger.debug("Notifying ALL observers:  nodeDataChanged:  path={}", path);
        try {
            Set<T> observerSet = getObserverSet(path, false);
            if (observerSet.size() > 0) {
                byte[] updatedData = zkClient.getData(path, true, new Stat());
                for (T observer : observerSet) {
                    synchronized (observer) {
                        if (!Arrays.equals(observer.getData(), updatedData)) {
                            byte[] previousData = observer.getData();
                            observer.setData(updatedData);
                            observer.nodeDataChanged(updatedData, previousData);
                        }
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
        String path = event.getPath();
        logger.debug("Notifying ALL observers:  nodeDeleted:  path={}", path);

        Set<T> observerSet = getObserverSet(path, false);
        if (observerSet.size() > 0) {
            // set up watch for when path comes back if there are observers
            try {
                zkClient.exists(path, true);
            } catch (Exception e) {
                logger.warn("Unable to set watch:  path=" + path, e);
            }

            for (T observer : observerSet) {

                logger.trace("Notifying observer:  observer.hashCode()={}", observer.hashCode());

                synchronized (observer) {
                    byte[] previousData = observer.getData();
                    observer.setData(null);

                    List<String> previousChildList = observer.getChildList();
                    observer.setChildList(Collections.EMPTY_LIST);

                    observer.nodeDeleted(previousData, previousChildList);
                }
            }
        }
    }

    public void removeAll(String path) {
        logger.debug("Removing ALL observers:  path={}", path);
        observerMap.remove(path);
    }

    public void remove(String path, Observer observer) {
        Set<T> observerSet = getObserverSet(path, false);
        boolean success = observerSet.remove(observer);

        logger.debug("Removed specific observer:  path={}; observer.hashCode()={}; success={}", new Object[] { path,
                observer.hashCode(), success });
    }

    public void signalStateReset(Object o) {
        logger.warn("Notifying ALL observers:  signalStateReset");
        for (String path : this.observerMap.keySet()) {
            Set<T> observerSet = getObserverSet(path, false);
            for (T observer : observerSet) {
                observer.stateReset(o);
            }
        }
    }

    public void signalStateUnknown(Object o) {
        logger.warn("Notifying ALL observers:  signalStateUnknown");
        for (String path : this.observerMap.keySet()) {
            Set<T> observerSet = getObserverSet(path, false);
            for (T observer : observerSet) {
                observer.stateUnknown(o);
            }
        }
    }

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
