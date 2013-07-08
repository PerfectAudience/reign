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

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class AbstractZkEventHandler implements ZkEventHandler {

    private static final Logger logger = LoggerFactory.getLogger(AbstractZkEventHandler.class);

    @Override
    public boolean filterWatchedEvent(WatchedEvent event) {
        return false;
    }

    @Override
    public void nodeChildrenChanged(WatchedEvent event) {
    }

    @Override
    public void nodeCreated(WatchedEvent event) {
    }

    @Override
    public void nodeDataChanged(WatchedEvent event) {
    }

    @Override
    public void nodeDeleted(WatchedEvent event) {
    }

    @Override
    public void connected(WatchedEvent event) {
    }

    @Override
    public void disconnected(WatchedEvent event) {
    }

    @Override
    public void sessionExpired(WatchedEvent event) {
    }

    @Override
    public void process(WatchedEvent event) {
        /** log event **/
        // log if TRACE
        if (logger.isTraceEnabled()) {
            logger.trace("***** Received ZooKeeper Event:  {}", ReflectionToStringBuilder.toString(event,
                    ToStringStyle.DEFAULT_STYLE));

        }

        /** check if we are filtering this event **/
        if (filterWatchedEvent(event)) {
            return;
        }

        /** process events **/
        switch (event.getType()) {
        case NodeChildrenChanged:
            nodeChildrenChanged(event);
            break;
        case NodeCreated:
            nodeCreated(event);
            break;
        case NodeDataChanged:
            nodeDataChanged(event);
            break;
        case NodeDeleted:
            nodeDeleted(event);
            break;
        case None:
            Event.KeeperState eventState = event.getState();
            if (eventState == Event.KeeperState.SyncConnected) {
                connected(event);
            } else if (eventState == Event.KeeperState.Disconnected) {
                disconnected(event);
            } else if (eventState == Event.KeeperState.Expired) {
                sessionExpired(event);
            } else {
                logger.warn("Unhandled event state:  eventType={}; eventState={}", event.getType(), event.getState());
            }
            break;
        default:
            logger.warn("Unhandled event type:  eventType={}; eventState={}", event.getType(), event.getState());
        }
    }
}
