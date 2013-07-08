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

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * Cleaner interface for handling ZooKeeper events.
 * 
 * @author ypai
 * 
 */
public interface ZkEventHandler extends Watcher {

    public void nodeChildrenChanged(WatchedEvent event);

    public void nodeCreated(WatchedEvent event);

    public void nodeDataChanged(WatchedEvent event);

    public void nodeDeleted(WatchedEvent event);

    public void connected(WatchedEvent event);

    public void disconnected(WatchedEvent event);

    public void sessionExpired(WatchedEvent event);

    /**
     * 
     * @param event
     * @return true if we do NOT need to process this event
     */
    public boolean filterWatchedEvent(WatchedEvent event);

}
