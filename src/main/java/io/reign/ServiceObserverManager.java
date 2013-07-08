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

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
public class ServiceObserverManager<T extends ServiceObserverWrapper> {

    private static final Logger logger = LoggerFactory.getLogger(ServiceObserverManager.class);

    private final ConcurrentMap<String, Set<T>> wrapperMap = new ConcurrentHashMap<String, Set<T>>(16, 0.9f, 2);

    public void put(String path, T observerWrapper) {
        Set<T> wrapperSet = getWrapperSet(path, true);
        wrapperSet.add(observerWrapper);

        logger.info("Added observer:  path={}; pathObserverCount={}", path, wrapperSet.size());
    }

    public Set<T> getObserverWrapperSet(String path) {
        Set<T> wrapperSet = getWrapperSet(path, false);
        return Collections.unmodifiableSet(wrapperSet);
    }

    // public void remove(String path, ServiceObserverWrapper observerWrapper) {
    // Set<T> wrapperSet = getWrapperSet(path, false);
    // wrapperSet.remove(observerWrapper);
    //
    // logger.info("Removed observer:  path={}; pathObserverCount={}", path, wrapperSet.size());
    // }

    public void removeAll(String path) {
        wrapperMap.remove(path);

        logger.info("Removed all observers:  path={}", path);
    }

    public void signalStateReset(Object o) {
        logger.info("Notifying ALL observers:  state unknown");
        for (String path : this.wrapperMap.keySet()) {
            Set<T> wrapperSet = getWrapperSet(path, false);
            for (T wrapper : wrapperSet) {
                wrapper.getObserver().stateReset(o);
            }
        }
    }

    public void signalStateUnknown(Object o) {
        logger.info("Notifying ALL observers:  state unknown");
        for (String path : this.wrapperMap.keySet()) {
            Set<T> wrapperSet = getWrapperSet(path, false);
            for (T wrapper : wrapperSet) {
                wrapper.getObserver().stateUnknown(o);
            }
        }
    }

    public void signal(String path, Object o) {
        Set<T> wrapperSet = getWrapperSet(path, false);

        logger.info("Notifying observers:  path={}; pathObserverCount={}", path, wrapperSet.size());

        for (T wrapper : wrapperSet) {
            wrapper.signalObserver(o);
        }
    }

    public boolean isBeingObserved(String path) {
        Set<T> wrapperSet = getWrapperSet(path, false);
        return wrapperSet.size() > 0;

    }

    Set<T> getWrapperSet(String path, boolean createIfNecessary) {
        Set<T> wrapperSet = wrapperMap.get(path);

        if (wrapperSet == null) {
            if (createIfNecessary) {
                Set<T> newWrapperSet = Collections.newSetFromMap(new ConcurrentHashMap<T, Boolean>(4, 0.9f, 2));
                wrapperSet = wrapperMap.putIfAbsent(path, newWrapperSet);
                if (wrapperSet == null) {
                    wrapperSet = newWrapperSet;
                }
            } else {
                wrapperSet = Collections.EMPTY_SET;
            }
        }

        return wrapperSet;
    }
}
