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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class ZkLockWatcher implements Watcher {
	private static final Logger logger = LoggerFactory.getLogger(ZkLockWatcher.class);

	private static AtomicInteger instancesOutstanding = new AtomicInteger(0);

	private final String lockPath;

	private final String lockReservationPath;

	public static long instancesOutstanding() {
		return instancesOutstanding.get();
	}

	public ZkLockWatcher(String lockPath, String lockReservationPath) {
		instancesOutstanding.incrementAndGet();

		this.lockPath = lockPath;
		this.lockReservationPath = lockReservationPath;

		if (logger.isDebugEnabled()) {
			logger.debug("Created:  instancesOutstanding={}; lockName={}; lockReservation={}", new Object[] {
			        instancesOutstanding.get(), lockPath, lockReservationPath });
		}
	}

	public void destroy() {
		// notify all waiters: shouldn't be any at this point
		synchronized (this) {
			this.notifyAll();
		}

		instancesOutstanding.decrementAndGet();
		if (logger.isDebugEnabled()) {
			logger.debug("Destroyed:  instancesOutstanding={}; lockName={}; lockReservation={}", new Object[] {
			        instancesOutstanding.get(), lockPath, lockReservationPath });
		}
	}

	public void waitForEvent(long waitTimeoutMs) throws InterruptedException {
		if (waitTimeoutMs == 0) {
			return;
		}

		logger.debug("waitForEvent():  instancesOutstanding={}; lockName={}; lockReservation={}", new Object[] {
		        instancesOutstanding.get(), lockPath, lockReservationPath });

		synchronized (this) {
			if (waitTimeoutMs == -1) {
				this.wait();

			} else {
				this.wait(waitTimeoutMs);
			}
		}
	}

	@Override
	public void process(WatchedEvent event) {
		// log if DEBUG
		if (logger.isDebugEnabled()) {
			logger.debug("***** Received ZooKeeper Event:  {}",
			        ReflectionToStringBuilder.toString(event, ToStringStyle.DEFAULT_STYLE));

		}

		// process events
		switch (event.getType()) {
		case NodeCreated:
		case NodeChildrenChanged:
		case NodeDataChanged:
		case NodeDeleted:
			synchronized (this) {
				this.notifyAll();

				if (logger.isDebugEnabled()) {
					logger.debug("Notifying threads waiting on LockWatcher:  lockWatcher.hashcode()=" + this.hashCode()
					        + "; instancesOutstanding=" + instancesOutstanding.get() + "; lockName=" + lockPath
					        + "; lockReservation=" + lockReservationPath);
				}
			}
			break;

		case None:
			Event.KeeperState eventState = event.getState();
			if (eventState == Event.KeeperState.SyncConnected) {
				// connection event: check children
				synchronized (this) {
					this.notifyAll();
				}

			} else if (eventState == Event.KeeperState.Disconnected || eventState == Event.KeeperState.Expired) {
				// disconnected: notifyAll so we can check children again on
				// reconnection
				synchronized (this) {
					this.notifyAll();
				}

			} else {
				logger.warn("Unhandled event state:  "
				        + ReflectionToStringBuilder.toString(event, ToStringStyle.DEFAULT_STYLE));
			}
			break;

		default:
			logger.warn("Unhandled event type:  "
			        + ReflectionToStringBuilder.toString(event, ToStringStyle.DEFAULT_STYLE));
		}

		// }// if

	}// process()
}// class
