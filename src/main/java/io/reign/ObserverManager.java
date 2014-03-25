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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Class for managing observers for services. Deals with multiple observers for a single path, etc.
 * 
 * Has a separate thread pool for dealing with observer callbacks, so as not to tie up the ZooKeeper event thread.
 * 
 * @author ypai
 * 
 * @param <T>
 *            the type of observer
 */
public class ObserverManager<T extends Observer> extends AbstractZkEventHandler {

    private static final Logger logger = LoggerFactory.getLogger(ObserverManager.class);

    private static final long DEFAULT_MAX_TIMEOUT_MILLIS = 120000;

    private final ConcurrentMap<String, Set<T>> observerMap = new ConcurrentHashMap<String, Set<T>>(16, 0.9f, 2);

    private final ZkClient zkClient;

    private ExecutorService executorService;

    private final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1);

    private int baseEventHandlerThreads = 2;
    private int maxEventHandlerThreads = 16;
    private int eventHandlingQueueMaxDepth = 128;
    private int threadTimeOutMillis = 60000;
    private volatile int sweeperInterval = 60000;

    public ObserverManager(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    public int getSweeperInterval() {
        return sweeperInterval;
    }

    public void setSweeperInterval(int sweeperInterval) {
        this.sweeperInterval = sweeperInterval;
    }

    public int getBaseEventHandlerThreads() {
        return baseEventHandlerThreads;
    }

    public void setBaseEventHandlerThreads(int baseEventHandlerThreads) {
        this.baseEventHandlerThreads = baseEventHandlerThreads;
    }

    public int getMaxEventHandlerThreads() {
        return maxEventHandlerThreads;
    }

    public void setMaxEventHandlerThreads(int maxEventHandlerThreads) {
        this.maxEventHandlerThreads = maxEventHandlerThreads;
    }

    public int getThreadTimeOutMillis() {
        return threadTimeOutMillis;
    }

    public void setThreadTimeOutMillis(int threadTimeOutMillis) {
        this.threadTimeOutMillis = threadTimeOutMillis;
    }

    public int getEventHandlingQueueMaxDepth() {
        return eventHandlingQueueMaxDepth;
    }

    public void setEventHandlingQueueMaxDepth(int eventHandlingQueueMaxDepth) {
        this.eventHandlingQueueMaxDepth = eventHandlingQueueMaxDepth;
    }

    // public ZkClient getZkClient() {
    // return zkClient;
    // }
    //
    // public void setZkClient(ZkClient zkClient) {
    // this.zkClient = zkClient;
    //
    // }

    public void init() {
        this.zkClient.register(this);
        executorService = new ThreadPoolExecutor(baseEventHandlerThreads, maxEventHandlerThreads, threadTimeOutMillis,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(eventHandlingQueueMaxDepth),
                new ThreadFactoryBuilder().setNameFormat("reign-observerManager-%d").build());
    }

    void update(String path, T observer) {
        // set again to make changes visible to other threads
        Set<T> observerSet = getObserverSet(path, true);
        observerSet.add(observer);
    }

    public void put(String path, T observer) {
        Set<T> observerSet = getObserverSet(path, true);
        try {
            // decorate observer with data about the path so we can handle notifications correctly
            byte[] data = zkClient.getData(path, true, new Stat());
            List<String> childList = zkClient.getChildren(path, true);

            observer.setPath(path);
            observer.setData(data);
            observer.setChildList(childList);
            observerSet.add(observer);

            logger.info("Added observer:  observer.hashCode()={}; path={}; pathObserverCount={}", new Object[] {
                    observer.hashCode(), path, observerSet.size() });
        } catch (KeeperException e) {
            if (e.code() == Code.NONODE) {
                // set up watch on that node
                try {
                    observer.setPath(path);
                    observer.setData(null);
                    observer.setChildList(Collections.EMPTY_LIST);
                    observerSet.add(observer);

                    zkClient.exists(path, true);

                    logger.info(
                            "Added observer for nonexistent path:  observer.hashCode()={}; path={}; pathObserverCount={}",
                            new Object[] { observer.hashCode(), path, observerSet.size() });
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

    public static interface RecheckedWatchedEvent {

    }

    public static class MarkedWatchedEvent extends WatchedEvent implements RecheckedWatchedEvent {
        MarkedWatchedEvent(WatchedEvent event) {
            super(event.getType(), event.getState(), event.getPath());
        }

        MarkedWatchedEvent(EventType eventType, KeeperState keeperState, String path) {
            super(eventType, keeperState, path);
        }
    }

    void scheduleCheck(final WatchedEvent event) {
        // do not schedule a check if we have already checked this one (to prevent infinite cycles from re-using
        // event handling methods for watched ZK events)
        if (event instanceof RecheckedWatchedEvent) {
            logger.trace("Ignoring:  already scheduled re-check:  path={}; eventType={}; intervalMillis={}",
                    event.getPath(), event.getType(), sweeperInterval);
            return;
        }

        logger.debug("Scheduling re-check after watch triggered:  path={}; eventType={}; intervalMillis={}",
                event.getPath(), event.getType(), sweeperInterval);

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                String path = event.getPath();
                try {
                    Stat stat = zkClient.exists(path, true);

                    switch (event.getType()) {
                    case NodeChildrenChanged:
                        if (stat != null) {
                            nodeChildrenChanged(new MarkedWatchedEvent(event));
                        }
                        break;
                    case NodeCreated:
                        if (stat == null) {
                            nodeDeleted(new MarkedWatchedEvent(event));
                        }
                        break;
                    case NodeDataChanged:
                        if (stat != null) {
                            nodeDataChanged(new MarkedWatchedEvent(event));
                        }
                        break;
                    case NodeDeleted:
                        if (stat != null) {
                            nodeCreated(new MarkedWatchedEvent(event));
                        }
                        break;
                    default:
                    }

                } catch (Exception e) {
                    logger.warn("Unable to check event:  path=" + path, e);
                }
            }

        }, this.sweeperInterval, TimeUnit.MILLISECONDS);
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
    public void nodeChildrenChanged(final WatchedEvent event) {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
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
                                List<String> previousChildList = observer.getChildList();
                                boolean updatedValueDiffers = childListsDiffer(updatedChildList, previousChildList);

                                observer.setChildList(updatedChildList);

                                update(path, observer);

                                if (updatedValueDiffers) {
                                    observer.nodeChildrenChanged(updatedChildList, previousChildList);
                                }
                            }
                        }// for

                        scheduleCheck(event);

                    }// if observerSet > 0
                } catch (KeeperException e) {
                    logger.warn("Unable to notify observers:  path=" + path, e);
                } catch (Exception e) {
                    logger.warn("Unable to notify observers:  path=" + path, e);
                }
            }
        });
    }

    boolean childListsDiffer(List<String> updatedChildList, List<String> previousChildList) {
        if (previousChildList == null) {
            previousChildList = Collections.EMPTY_LIST;
        }
        if (updatedChildList == null) {
            updatedChildList = Collections.EMPTY_LIST;
        }

        boolean updatedValueDiffers = previousChildList.size() != updatedChildList.size();
        if (updatedValueDiffers) {
            return true;
        } else {
            // if sizes are the same, compare contents independent of order
            Set<String> childListSet = new HashSet<String>(previousChildList.size() + 1, 1.0f);
            childListSet.addAll(previousChildList);

            Set<String> updatedChildListSet = new HashSet<String>(updatedChildList.size() + 1, 1.0f);
            updatedChildListSet.addAll(updatedChildList);

            updatedValueDiffers = childListSet.equals(updatedChildListSet);
        }

        return updatedValueDiffers;
    }

    @Override
    public void nodeCreated(final WatchedEvent event) {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                String path = event.getPath();
                logger.debug("Notifying ALL observers:  nodeCreated:  path={}", path);
                try {
                    Set<T> observerSet = getObserverSet(path, false);
                    if (observerSet.size() > 0) {
                        // get children just to get a child watch
                        List<String> childList = zkClient.getChildren(path, true);
                        byte[] data = zkClient.getData(path, true, new Stat());

                        for (T observer : observerSet) {
                            logger.trace("Notifying observer:  observer.hashCode()={}", observer.hashCode());
                            synchronized (observer) {
                                byte[] previousData = observer.getData();
                                List<String> previousChildList = observer.getChildList();
                                observer.setData(data);
                                observer.setChildList(childList);

                                update(path, observer);

                                if (previousData == null && previousChildList == Collections.EMPTY_LIST) {
                                    observer.nodeCreated(data, childList);
                                }
                            }
                        }

                        scheduleCheck(event);
                    }
                } catch (KeeperException e) {
                    logger.warn("Unable to notify observers:  path=" + path, e);
                } catch (Exception e) {
                    logger.warn("Unable to notify observers:  path=" + path, e);
                }
            }
        });
    }

    @Override
    public void nodeDataChanged(final WatchedEvent event) {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                String path = event.getPath();
                logger.debug("Notifying ALL observers:  nodeDataChanged:  path={}", path);
                try {
                    Set<T> observerSet = getObserverSet(path, false);
                    if (observerSet.size() > 0) {
                        byte[] updatedData = zkClient.getData(path, true, new Stat());
                        for (T observer : observerSet) {
                            logger.trace("Notifying observer:  observer.hashCode()={}", observer.hashCode());
                            synchronized (observer) {
                                if (!Arrays.equals(observer.getData(), updatedData)) {
                                    byte[] previousData = observer.getData();
                                    observer.setData(updatedData);

                                    update(path, observer);

                                    observer.nodeDataChanged(updatedData, previousData);
                                }
                            }
                        }

                        scheduleCheck(event);
                    }
                } catch (KeeperException e) {
                    logger.warn("Unable to notify observers:  path=" + path, e);
                } catch (Exception e) {
                    logger.warn("Unable to notify observers:  path=" + path, e);
                }
            }
        });
    }

    @Override
    public void nodeDeleted(final WatchedEvent event) {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
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

                            update(path, observer);

                            if (previousChildList != observer.getChildList() || previousData != observer.getData()) {
                                observer.nodeDeleted(previousData, previousChildList);
                            }
                        }
                    }

                    scheduleCheck(event);

                }// if observerSet>0
            }
        });
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

    public void signalStateReset(final Object o) {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                logger.warn("Notifying ALL observers:  signalStateReset");
                for (String path : observerMap.keySet()) {
                    Set<T> observerSet = getObserverSet(path, false);
                    for (T observer : observerSet) {
                        observer.stateReset(o);
                    }
                }
            }
        });
    }

    public void signalStateUnknown(final Object o) {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                logger.warn("Notifying ALL observers:  signalStateUnknown");
                for (String path : observerMap.keySet()) {
                    Set<T> observerSet = getObserverSet(path, false);
                    for (T observer : observerSet) {
                        observer.stateUnknown(o);
                    }
                }
            }
        });
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
