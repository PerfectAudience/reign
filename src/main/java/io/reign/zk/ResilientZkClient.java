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

package io.reign.zk;

import io.reign.ZkClient;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.zookeeper.AsyncCallback.ACLCallback;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replacement for the ZooKeeper class that offers retry and re-connects when there are session failures.
 * 
 * Also allows registration of additional Watcher(s).
 * 
 * @author ypai
 * 
 */
public class ResilientZkClient implements ZkClient, Watcher {

	private static final Logger logger = LoggerFactory.getLogger(ResilientZkClient.class);

	private final BackoffStrategyFactory DEFAULT_BACKOFF_STRATEGY_FACTORY = new ExponentialBackoffStrategyFactory(1000,
	        30000, true);

	private volatile BackoffStrategyFactory backoffStrategyFactory = DEFAULT_BACKOFF_STRATEGY_FACTORY;

	private volatile Long currentSessionId;
	private volatile byte[] sessionPassword;

	private volatile ZooKeeper zooKeeper;

	private volatile boolean connected = false;

	/** Map of String path to Set of unique Watcher(s): used to track child watches */
	private final ConcurrentMap<String, Set<Watcher>> childWatchesMap = new ConcurrentHashMap<String, Set<Watcher>>(
	        256, 0.9f, 2);

	/** Map of String path to Set of unique Watcher(s): used to track data watches */
	private final ConcurrentMap<String, Set<Watcher>> dataWatchesMap = new ConcurrentHashMap<String, Set<Watcher>>(256,
	        0.9f, 2);

	private final Set<Watcher> watcherSet = Collections.newSetFromMap(new ConcurrentHashMap<Watcher, Boolean>(32, 0.9f,
	        1));

	private String connectString;

	private int sessionTimeoutMillis;

	private static long ASSUME_ERROR_TIMEOUT_MS = 60000;

	/** when true, we do not attempt reconnect on failure */
	private volatile boolean shutdown = false;

	// /** object to synchronize on for connection/re-connection attempts */
	// private final ReentrantLock connectionLock = new ReentrantLock();

	public ResilientZkClient(String connectString, int sessionTimeoutMillis, long sessionId, byte[] sessionPassword)
	        throws IOException {
		this.connectString = connectString;
		this.sessionTimeoutMillis = sessionTimeoutMillis;
		this.currentSessionId = sessionId;
		this.sessionPassword = sessionPassword;
		this.zooKeeper = new ZooKeeper(connectString, sessionTimeoutMillis, this, sessionId, sessionPassword);
	}

	public ResilientZkClient(String connectString, int sessionTimeoutMillis) throws IOException {
		this.connectString = connectString;
		this.sessionTimeoutMillis = sessionTimeoutMillis;
		this.zooKeeper = new ZooKeeper(connectString, sessionTimeoutMillis, this);
	}

	public BackoffStrategyFactory getBackoffStrategyFactory() {
		return backoffStrategyFactory;
	}

	public void setBackoffStrategyFactory(BackoffStrategyFactory backoffStrategyFactory) {
		this.backoffStrategyFactory = backoffStrategyFactory;
	}

	public int getSessionTimeout() {
		return sessionTimeoutMillis;
	}

	public void setSessionTimeout(int sessionTimeout) {
		this.sessionTimeoutMillis = sessionTimeout;
	}

	public String getConnectString() {
		return connectString;
	}

	public void setConnectString(String connectString) {
		this.connectString = connectString;
	}

	public void addAuthInfo(String scheme, byte[] auth) {

		this.zooKeeper.addAuthInfo(scheme, auth);
	}

	/**
	 * getData() and exists() set data watches.
	 * 
	 * @param path
	 * @param watcher
	 */
	void trackDataWatch(String path, Watcher watcher) {
		Set<Watcher> watcherSet = dataWatchesMap.get(path);
		if (watcherSet == null) {
			Set<Watcher> newWatcherSet = Collections.newSetFromMap(new ConcurrentHashMap<Watcher, Boolean>(4, 0.9f, 1));
			watcherSet = dataWatchesMap.putIfAbsent(path, newWatcherSet);
			if (watcherSet == null) {
				watcherSet = newWatcherSet;
			}
		}
		watcherSet.add(watcher);
	}

	/**
	 * getChildren() sets child watches
	 * 
	 * @param path
	 * @param watcher
	 */
	void trackChildWatch(String path, Watcher watcher) {
		Set<Watcher> watcherSet = childWatchesMap.get(path);
		if (watcherSet == null) {
			Set<Watcher> newWatcherSet = Collections.newSetFromMap(new ConcurrentHashMap<Watcher, Boolean>(4, 0.9f, 1));
			watcherSet = childWatchesMap.putIfAbsent(path, newWatcherSet);
			if (watcherSet == null) {
				watcherSet = newWatcherSet;
			}
		}
		watcherSet.add(watcher);
	}

	/**
	 * Re-establish any existing ZooKeeper watchers after reconnection.
	 */
	void restoreWatches() {
		for (String path : dataWatchesMap.keySet()) {
			logger.info("Restoring data watch(s):  path={}", path);
			for (Watcher watcher : dataWatchesMap.get(path)) {
				try {
					this.exists(path, watcher);
				} catch (KeeperException e) {
					if (e.code() != Code.NONODE) {
						logger.warn("Error while restoring watch:  " + e + ":  path=" + path, e);
					}
				} catch (Exception e) {
					logger.warn("Error while restoring watch:  " + e + ":  path=" + path, e);
				} // try
			}// for
		}// for

		for (String path : childWatchesMap.keySet()) {
			logger.info("Restoring child watch(s):  path={}", path);
			for (Watcher watcher : childWatchesMap.get(path)) {
				try {
					this.getChildren(path, watcher);
				} catch (KeeperException e) {
					if (e.code() != Code.NONODE) {
						logger.warn("Error while restoring watch:  " + e + ":  path=" + path, e);
					}
				} catch (Exception e) {
					logger.warn("Error while restoring watch:  " + e + ":  path=" + path, e);
				} // try
			}// for
		}
	}

	@Override
	public synchronized void close() {

		this.shutdown = true;

		if (this.zooKeeper != null) {
			try {
				if (logger.isInfoEnabled()) {
					logger.info("Closing ZooKeeper session:  sessionId={}; connectString={}", currentSessionId,
					        getConnectString());
				}
				this.zooKeeper.close();
				this.connected = false;

				// notify any waiters
				this.notifyAll();
			} catch (InterruptedException e) {
				logger.warn("Sleep interrupted while closing existing ZooKeeper session:  " + e, e);
			} // try
		}// if

	}

	public void create(final String path, final byte[] data, final List<ACL> acl, final CreateMode createMode,
	        final StringCallback cb, final Object ctx) {

		VoidZooKeeperAction zkAction = new VoidZooKeeperAction(backoffStrategyFactory.get()) {
			@Override
			public void doPerform() throws KeeperException, InterruptedException {
				zooKeeper.create(path, data, acl, createMode, cb, ctx);

			}
		};

		try {
			zkAction.perform();
		} catch (InterruptedException e) {
			logger.error("getData():  " + e, e);
		} catch (KeeperException e) {
			logger.error("getData():  " + e, e);
		}

	}

	public void delete(final String path, final int version, final VoidCallback cb, final Object ctx) {

		VoidZooKeeperAction zkAction = new VoidZooKeeperAction(backoffStrategyFactory.get()) {
			@Override
			public void doPerform() throws KeeperException, InterruptedException {
				zooKeeper.delete(path, version, cb, ctx);
			}
		};

		try {
			zkAction.perform();
		} catch (InterruptedException e) {
			logger.error("getData():  " + e, e);
		} catch (KeeperException e) {
			logger.error("getData():  " + e, e);
		}

	}

	public void exists(final String path, final boolean watch, final StatCallback cb, final Object ctx) {
		if (watch) {
			trackDataWatch(path, this);
		}

		VoidZooKeeperAction zkAction = new VoidZooKeeperAction(backoffStrategyFactory.get()) {
			@Override
			public void doPerform() throws KeeperException, InterruptedException {
				zooKeeper.exists(path, watch, cb, ctx);

			}
		};

		try {
			zkAction.perform();
		} catch (InterruptedException e) {
			logger.error("getData():  " + e, e);
		} catch (KeeperException e) {
			logger.error("getData():  " + e, e);
		}

	}

	public void exists(final String path, final Watcher watcher, final StatCallback cb, final Object ctx) {
		if (watcher != null) {
			trackDataWatch(path, watcher);
		}

		VoidZooKeeperAction zkAction = new VoidZooKeeperAction(backoffStrategyFactory.get()) {
			@Override
			public void doPerform() throws KeeperException, InterruptedException {
				zooKeeper.exists(path, watcher, cb, ctx);

			}
		};

		try {
			zkAction.perform();
		} catch (InterruptedException e) {
			logger.error("getData():  " + e, e);
		} catch (KeeperException e) {
			logger.error("getData():  " + e, e);
		}

	}

	public void getACL(final String path, final Stat stat, final ACLCallback cb, final Object ctx) {
		VoidZooKeeperAction zkAction = new VoidZooKeeperAction(backoffStrategyFactory.get()) {
			@Override
			public void doPerform() throws KeeperException, InterruptedException {
				zooKeeper.getACL(path, stat, cb, ctx);

			}
		};

		try {
			zkAction.perform();
		} catch (InterruptedException e) {
			logger.error("getData():  " + e, e);
		} catch (KeeperException e) {
			logger.error("getData():  " + e, e);
		}

	}

	public List<ACL> getACL(final String path, final Stat stat) throws KeeperException, InterruptedException {

		ZooKeeperAction<List<ACL>> zkAction = new ZooKeeperAction<List<ACL>>(backoffStrategyFactory.get()) {
			@Override
			public List<ACL> doPerform() throws KeeperException, InterruptedException {
				return zooKeeper.getACL(path, stat);

			}
		};

		return zkAction.perform();
	}

	public void getChildren(final String path, final boolean watch, final Children2Callback cb, final Object ctx) {
		if (watch) {
			trackChildWatch(path, this);
		}

		VoidZooKeeperAction zkAction = new VoidZooKeeperAction(backoffStrategyFactory.get()) {
			@Override
			public void doPerform() throws KeeperException, InterruptedException {
				zooKeeper.getChildren(path, watch, cb, ctx);

			}
		};

		try {
			zkAction.perform();
		} catch (InterruptedException e) {
			logger.error("getChildren():  " + e, e);
		} catch (KeeperException e) {
			logger.error("getChildren():  " + e, e);
		}

	}

	public void getChildren(final String path, final boolean watch, final ChildrenCallback cb, final Object ctx) {

		if (watch) {
			trackChildWatch(path, this);
		}

		VoidZooKeeperAction zkAction = new VoidZooKeeperAction(backoffStrategyFactory.get()) {
			@Override
			public void doPerform() throws KeeperException, InterruptedException {
				zooKeeper.getChildren(path, watch, cb, ctx);

			}
		};

		try {
			zkAction.perform();
		} catch (InterruptedException e) {
			logger.error("getChildren():  " + e, e);
		} catch (KeeperException e) {
			logger.error("getChildren():  " + e, e);
		}

	}

	@Override
	public List<String> getChildren(final String path, final boolean watch, final Stat stat) throws KeeperException,
	        InterruptedException {

		if (watch) {
			trackChildWatch(path, this);
		}

		ZooKeeperAction<List<String>> zkAction = new ZooKeeperAction<List<String>>(backoffStrategyFactory.get()) {
			@Override
			public List<String> doPerform() throws KeeperException, InterruptedException {
				try {
					return zooKeeper.getChildren(path, watch, stat);
				} catch (KeeperException e) {
					if (e.code() != Code.NONODE) {
						throw e;
					}
					return Collections.EMPTY_LIST;
				}

			}
		};

		return zkAction.perform();
	}

	public void getChildren(final String path, final Watcher watcher, final Children2Callback cb, final Object ctx) {
		if (watcher != null) {
			trackChildWatch(path, watcher);
		}

		VoidZooKeeperAction zkAction = new VoidZooKeeperAction(backoffStrategyFactory.get()) {
			@Override
			public void doPerform() throws KeeperException, InterruptedException {
				zooKeeper.getChildren(path, watcher, cb, ctx);

			}
		};

		try {
			zkAction.perform();
		} catch (InterruptedException e) {
			logger.error("getChildren():  " + e, e);
		} catch (KeeperException e) {
			logger.error("getChildren():  " + e, e);
		}

	}

	public void getChildren(final String path, final Watcher watcher, final ChildrenCallback cb, final Object ctx) {
		if (watcher != null) {
			trackChildWatch(path, watcher);
		}

		VoidZooKeeperAction zkAction = new VoidZooKeeperAction(backoffStrategyFactory.get()) {
			@Override
			public void doPerform() throws KeeperException, InterruptedException {
				zooKeeper.getChildren(path, watcher, cb, ctx);

			}
		};

		try {
			zkAction.perform();
		} catch (InterruptedException e) {
			logger.error("getChildren():  " + e, e);
		} catch (KeeperException e) {
			logger.error("getChildren():  " + e, e);
		}

	}

	public List<String> getChildren(final String path, final Watcher watcher, final Stat stat) throws KeeperException,
	        InterruptedException {

		if (watcher != null) {
			trackChildWatch(path, watcher);
		}

		ZooKeeperAction<List<String>> zkAction = new ZooKeeperAction<List<String>>(backoffStrategyFactory.get()) {
			@Override
			public List<String> doPerform() throws KeeperException, InterruptedException {
				try {
					return zooKeeper.getChildren(path, watcher, stat);
				} catch (KeeperException e) {
					if (e.code() != Code.NONODE) {
						throw e;
					}
					return Collections.EMPTY_LIST;
				}
			}
		};

		return zkAction.perform();
	}

	@Override
	public List<String> getChildren(final String path, final Watcher watcher) throws KeeperException,
	        InterruptedException {

		if (watcher != null) {
			trackChildWatch(path, watcher);
		}

		ZooKeeperAction<List<String>> zkAction = new ZooKeeperAction<List<String>>(backoffStrategyFactory.get()) {
			@Override
			public List<String> doPerform() throws KeeperException, InterruptedException {
				try {
					return zooKeeper.getChildren(path, watcher);
				} catch (KeeperException e) {
					if (e.code() != Code.NONODE) {
						throw e;
					}
					return Collections.EMPTY_LIST;
				}

			}
		};

		return zkAction.perform();
	}

	public void getData(final String path, final boolean watch, final DataCallback cb, final Object ctx) {

		if (watch) {
			trackDataWatch(path, this);
		}

		VoidZooKeeperAction zkAction = new VoidZooKeeperAction(backoffStrategyFactory.get()) {
			@Override
			public void doPerform() throws KeeperException, InterruptedException {
				zooKeeper.getData(path, watch, cb, ctx);

			}
		};

		try {
			zkAction.perform();
		} catch (InterruptedException e) {
			logger.error("getData():  " + e, e);
		} catch (KeeperException e) {
			logger.error("getData():  " + e, e);
		}

	}

	public void getData(final String path, final Watcher watcher, final DataCallback cb, final Object ctx) {

		if (watcher != null) {
			trackDataWatch(path, watcher);
		}

		VoidZooKeeperAction zkAction = new VoidZooKeeperAction(backoffStrategyFactory.get()) {
			@Override
			public void doPerform() throws KeeperException, InterruptedException {
				zooKeeper.getData(path, watcher, cb, ctx);

			}
		};

		try {
			zkAction.perform();
		} catch (InterruptedException e) {
			logger.error("getData():  " + e, e);
		} catch (KeeperException e) {
			logger.error("getData():  " + e, e);
		}

	}

	public byte[] getData(final String path, final Watcher watcher, final Stat stat) throws KeeperException,
	        InterruptedException {

		if (watcher != null) {
			trackDataWatch(path, watcher);
		}

		ZooKeeperAction<byte[]> zkAction = new ZooKeeperAction<byte[]>(backoffStrategyFactory.get()) {
			@Override
			public byte[] doPerform() throws KeeperException, InterruptedException {
				return zooKeeper.getData(path, watcher, stat);

			}
		};

		return zkAction.perform();
	}

	public long getSessionId() {
		return zooKeeper.getSessionId();
	}

	public byte[] getSessionPasswd() {
		return zooKeeper.getSessionPasswd();
	}

	@Override
	public void register(Watcher watcher) {
		this.watcherSet.add(watcher);
		logger.info("Registered watcher:  {}:  hashCode={}; watchers={}", new Object[] { watcher.getClass().getName(),
		        watcher.hashCode(), watcherSet.size() });
	}

	public void setACL(final String path, final List<ACL> acl, final int version, final StatCallback cb,
	        final Object ctx) {

		VoidZooKeeperAction zkAction = new VoidZooKeeperAction(backoffStrategyFactory.get()) {
			@Override
			public void doPerform() throws KeeperException, InterruptedException {
				zooKeeper.setACL(path, acl, version, cb, ctx);

			}
		};

		try {
			zkAction.perform();
		} catch (InterruptedException e) {
			logger.error("setACL():  " + e, e);
		} catch (KeeperException e) {
			logger.error("setACL():  " + e, e);
		}

	}

	public Stat setACL(final String path, final List<ACL> acl, final int version) throws KeeperException,
	        InterruptedException {

		ZooKeeperAction<Stat> zkAction = new ZooKeeperAction<Stat>(backoffStrategyFactory.get()) {
			@Override
			public Stat doPerform() throws KeeperException, InterruptedException {
				return zooKeeper.setACL(path, acl, version);

			}
		};

		return zkAction.perform();
	}

	/**
	 * 
	 * @param path
	 * @param data
	 * @param version
	 * @param cb
	 * @param ctx
	 */
	public void setData(final String path, final byte[] data, final int version, final StatCallback cb, final Object ctx) {
		VoidZooKeeperAction zkAction = new VoidZooKeeperAction(backoffStrategyFactory.get()) {
			@Override
			public void doPerform() throws KeeperException, InterruptedException {
				zooKeeper.setData(path, data, version, cb, ctx);

			}
		};

		try {
			zkAction.perform();
		} catch (InterruptedException e) {
			logger.error("setData():  " + e, e);
		} catch (KeeperException e) {
			logger.error("setData():  " + e, e);
		}
	}

	/**
	 * From ZK docs:
	 * 
	 * Sometimes developers mistakenly assume one other guarantee that ZooKeeper does not in fact make. This is:
	 * Simultaneously Consistent Cross-Client Views ZooKeeper does not guarantee that at every instance in time, two
	 * different clients will have identical views of ZooKeeper data. Due to factors like network delays, one client may
	 * perform an update before another client gets notified of the change. Consider the scenario of two clients, A and
	 * B. If client A sets the value of a znode /a from 0 to 1, then tells client B to read /a, client B may read the
	 * old value of 0, depending on which server it is connected to. If it is important that Client A and Client B read
	 * the same value, Client B should should call the sync() method from the ZooKeeper API method before it performs
	 * its read.
	 * 
	 * 
	 * @param path
	 * @param cb
	 * @param ctx
	 */
	@Override
	public void sync(final String path, final VoidCallback cb, final Object ctx) {
		VoidZooKeeperAction zkAction = new VoidZooKeeperAction(backoffStrategyFactory.get()) {

			@Override
			public void doPerform() throws KeeperException, InterruptedException {
				zooKeeper.sync(path, cb, ctx);

			}

		};

		try {
			zkAction.perform();
		} catch (InterruptedException e) {
			logger.error("sync():  " + e, e);
		} catch (KeeperException e) {
			logger.error("sync():  " + e, e);
		}
	}

	/**
	 * 
	 * @param path
	 * @param data
	 * @param acl
	 * @param createMode
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	@Override
	public String create(final String path, final byte[] data, final List<ACL> acl, final CreateMode createMode)
	        throws KeeperException, InterruptedException {

		ZooKeeperAction<String> zkAction = new ZooKeeperAction<String>(backoffStrategyFactory.get()) {

			@Override
			public String doPerform() throws KeeperException, InterruptedException {
				return zooKeeper.create(path, data, acl, createMode);

			}

		};

		String pathCreated = zkAction.perform();

		if (logger.isDebugEnabled()) {
			logger.debug("create():  Path created:  pathCreated={}", pathCreated);
		}

		return pathCreated;

	}

	/**
	 * 
	 * @return
	 */
	public ZooKeeper.States getState() {

		ZooKeeperAction<ZooKeeper.States> zkAction = new ZooKeeperAction<ZooKeeper.States>(backoffStrategyFactory.get()) {
			@Override
			public ZooKeeper.States doPerform() throws KeeperException, InterruptedException {
				// if we are not connected
				if (!shutdown) {
					return zooKeeper.getState();
				} else {
					return null;
				}
			}
		};

		ZooKeeper.States state = null;
		try {
			state = zkAction.perform();
		} catch (InterruptedException e) {
			logger.error("getState():  " + e, e);
		} catch (KeeperException e) {
			logger.error("getState():  " + e, e);
		}

		return state;

	}

	/**
	 * 
	 * @param path
	 * @param watch
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	@Override
	public Stat exists(final String path, final boolean watch) throws KeeperException, InterruptedException {

		if (watch) {
			trackDataWatch(path, this);
		}

		ZooKeeperAction<Stat> zkAction = new ZooKeeperAction<Stat>(backoffStrategyFactory.get()) {
			@Override
			public Stat doPerform() throws KeeperException, InterruptedException {
				return zooKeeper.exists(path, watch);
			}

		};
		return zkAction.perform();

	}

	/**
	 * 
	 * @param path
	 * @param watch
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	@Override
	public List<String> getChildren(final String path, final boolean watch) throws KeeperException,
	        InterruptedException {

		if (watch) {
			trackChildWatch(path, this);
		}

		ZooKeeperAction<List<String>> zkAction = new ZooKeeperAction<List<String>>(backoffStrategyFactory.get()) {

			@Override
			public List<String> doPerform() throws KeeperException, InterruptedException {
				try {
					return zooKeeper.getChildren(path, watch);
				} catch (KeeperException e) {
					if (e.code() != Code.NONODE) {
						throw e;
					}
					return Collections.EMPTY_LIST;
				}
			}

		};
		return zkAction.perform();

	}

	/**
	 * 
	 * @param path
	 * @param version
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	@Override
	public void delete(final String path, final int version) throws InterruptedException, KeeperException {

		VoidZooKeeperAction zkAction = new VoidZooKeeperAction(backoffStrategyFactory.get()) {

			@Override
			public void doPerform() throws KeeperException, InterruptedException {
				try {
					zooKeeper.delete(path, version);
				} catch (KeeperException e) {
					if (e.code() != Code.NONODE) {
						throw e;
					}
				}
			}

		};
		zkAction.perform();

	}

	/**
	 * 
	 * @param path
	 * @param data
	 * @param version
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	@Override
	public Stat setData(final String path, final byte[] data, final int version) throws KeeperException,
	        InterruptedException {

		ZooKeeperAction<Stat> zkAction = new ZooKeeperAction<Stat>(backoffStrategyFactory.get()) {

			@Override
			public Stat doPerform() throws KeeperException, InterruptedException {
				Stat stat = zooKeeper.setData(path, data, version);
				return stat;
			}

		};
		return zkAction.perform();

	}

	/**
	 * 
	 * @param path
	 * @param watch
	 * @param stat
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	@Override
	public byte[] getData(final String path, final boolean watch, final Stat stat) throws KeeperException,
	        InterruptedException {

		if (watch) {
			trackDataWatch(path, this);
		}

		ZooKeeperAction<byte[]> zkAction = new ZooKeeperAction<byte[]>(backoffStrategyFactory.get()) {

			@Override
			public byte[] doPerform() throws KeeperException, InterruptedException {
				byte[] data = zooKeeper.getData(path, watch, stat);
				return data;
			}

		};
		return zkAction.perform();

	}

	/**
	 * 
	 * @param path
	 * @param watcher
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	@Override
	public Stat exists(final String path, final Watcher watcher) throws KeeperException, InterruptedException {

		if (watcher != null) {
			trackDataWatch(path, watcher);
		}

		ZooKeeperAction<Stat> zkAction = new ZooKeeperAction<Stat>(backoffStrategyFactory.get()) {

			@Override
			public Stat doPerform() throws KeeperException, InterruptedException {
				Stat stat = zooKeeper.exists(path, watcher);
				return stat;
			}

		};
		return zkAction.perform();

	}

	/**
     * 
     */
	synchronized void connect(BackoffStrategy backoffStrategy, boolean force) {
		/**
		 * explicitly set connected flag to false so we will close current connection and attempt reconnect
		 **/
		if (force) {
			this.connected = false;
			this.currentSessionId = null;
		}

		/** attempt reconnection if necessary **/
		while (!connected && !this.shutdown) {
			// close existing ZK connection if necessary
			if (this.zooKeeper != null) {
				try {
					if (logger.isInfoEnabled()) {
						logger.info("Closing ZooKeeper session:  currentSessionId={}; connectString={}",
						        currentSessionId, getConnectString());
					}

					this.zooKeeper.close();
				} catch (InterruptedException e) {
					logger.warn("Sleep interrupted while closing existing ZooKeeper session:  " + e, e);
				} // try
			}

			// reconnect to ZK
			try {
				if (logger.isInfoEnabled()) {
					logger.info("Connecting to ZooKeeper:  currentSessionId={}; connectString={}", currentSessionId,
					        getConnectString());
				}
				if (currentSessionId == null) {
					this.zooKeeper = new ZooKeeper(getConnectString(), getSessionTimeout(), this);
				} else {
					this.zooKeeper = new ZooKeeper(getConnectString(), getSessionTimeout(), this, currentSessionId,
					        sessionPassword);
				}

				synchronized (this) {
					this.wait(ASSUME_ERROR_TIMEOUT_MS + 10000);
				}

			} catch (Exception e) {
				if (backoffStrategy.next() == null) {
					break;
				}

				logger.error("Could not reconnect to ZooKeeper (retrying in " + backoffStrategy.get() + " ms):  " + e,
				        e);

				try {
					Thread.sleep(backoffStrategy.get());
				} catch (InterruptedException e1) {
					logger.warn("Sleep interrupted while sleeping between attempts to reconnect to ZooKeeper:  " + e, e);
				}
			}// try
		}// while

	} // connect()

	@Override
	public void process(final WatchedEvent event) {
		/***** log event *****/
		// log if TRACE
		if (logger.isTraceEnabled()) {
			logger.trace("***** Received ZooKeeper Event:  {}",
			        ReflectionToStringBuilder.toString(event, ToStringStyle.DEFAULT_STYLE));

		}

		/***** pass event on to registered Watchers *****/
		if (event.getType() != EventType.None) {
			if (shutdown) {
				logger.warn("Already shutdown:  not passing event to registered watchers:  type={}; path={}",
				        event.getType(), event.getPath());
				return;
			}

			for (Watcher watcher : watcherSet) {
				watcher.process(event);
			}
		}

		/***** process events *****/
		switch (event.getType()) {
		case NodeChildrenChanged:
			this.childWatchesMap.remove(event.getPath());
			break;
		case NodeCreated:
		case NodeDataChanged:
		case NodeDeleted:
			this.dataWatchesMap.remove(event.getPath());
			break;
		case None:
			Event.KeeperState eventState = event.getState();
			if (eventState == Event.KeeperState.SyncConnected) {

				this.connected = true;

				if (currentSessionId == null) {
					if (logger.isInfoEnabled()) {
						logger.info(
						        "Restoring watches as necessary:  sessionId={}; connectString={}; sessionTimeout={}",
						        new Object[] { this.zooKeeper.getSessionId(), getConnectString(), getSessionTimeout() });
					}
					restoreWatches();
				}

				this.currentSessionId = this.zooKeeper.getSessionId();
				this.sessionPassword = this.zooKeeper.getSessionPasswd();

				logger.info("SyncConnected:  notifying all waiters:  currentSessionId={}; connectString={}",
				        currentSessionId, getConnectString());
				synchronized (this) {
					// notify waiting threads that connection has been
					// established
					this.notifyAll();
				}
				logger.info("SyncConnected:  notified all waiters:  currentSessionId={}; connectString={}",
				        currentSessionId, getConnectString());

			} else if (eventState == Event.KeeperState.Disconnected) {
				this.connected = false;

			} else if (eventState == Event.KeeperState.Expired) {
				// expired session; close ZK connection and reconnect
				if (!this.shutdown) {
					// if session has been expired, clear out the existing ID
					logger.info(
					        "Session has been expired by ZooKeeper cluster:  reconnecting to establish new session:  oldSessionId={}; connectString={}",
					        currentSessionId, getConnectString());

					// null out current session ID
					this.currentSessionId = null;

					// do connection in another thread so as to not block the ZK event thread
					Thread reconnectThread = new Thread() {
						@Override
						public void run() {
							connect(backoffStrategyFactory.get(), true);
						}
					};
					reconnectThread.setName(this.getClass().getSimpleName() + ".zkConnectThread-"
					        + reconnectThread.hashCode());
					reconnectThread.setPriority(Thread.MIN_PRIORITY);
					reconnectThread.run();
				}

			} else {
				logger.warn("Unhandled state:  eventType=" + event.getType() + "; eventState=" + eventState);
			}
			break;
		default:
			logger.warn("Unhandled event type:  eventType=" + event.getType() + "; eventState=" + event.getState());
		}

	}// process

	/**
	 * 
	 * @param errorCode
	 * @return true if exception can be classified as a ZK session error
	 */
	private boolean isZooKeeperSessionError(KeeperException.Code errorCode) {
		return (errorCode == KeeperException.Code.CONNECTIONLOSS
		        || errorCode == KeeperException.Code.RUNTIMEINCONSISTENCY
		        || errorCode == KeeperException.Code.SESSIONEXPIRED || errorCode == KeeperException.Code.SYSTEMERROR || errorCode == KeeperException.Code.SESSIONMOVED);
	}// isZooKeeperSessionError

	/**
     * 
     */
	void awaitConnectionInitialization(BackoffStrategy backoffStrategy) {
		while (!this.shutdown && (zooKeeper == null || !this.connected)) {
			try {
				logger.debug("Waiting for ZooKeeper connection to be established...");
				if (backoffStrategy.next() == null) {
					break;
				}
				synchronized (this) {
					wait(backoffStrategy.get());
				}
			} catch (InterruptedException e) {
				logger.info("Interrupted waiting for ZooKeeper connection to be established...");
			}
		}// while
	}

	/**
	 * 
	 * @param backoffStrategy
	 * @param e
	 * @throws KeeperException
	 */
	void handleKeeperException(BackoffStrategy backoffStrategy, KeeperException e) throws KeeperException {
		if (shutdown) {
			throw e;
		}

		if (!backoffStrategy.hasNext()) {
			// just throw exception if in fail fast mode
			throw e;
		} else if (this.isZooKeeperSessionError(e.code())) {
			// if it is a ZK session error, await connection renewal
			awaitConnectionInitialization(backoffStrategyFactory.get());
		} else {
			throw e;
		}// if
	}

	/**
	 * 
	 * @author ypai
	 * 
	 * @param <T>
	 */
	public abstract class ZooKeeperAction<T> {

		/** backoff strategy to use on reconnection attempts */
		private final BackoffStrategy _backoffStrategy;

		public ZooKeeperAction(BackoffStrategy _backoffStrategy) {
			this._backoffStrategy = _backoffStrategy;
		}

		/**
		 * 
		 * @return
		 * @throws KeeperException
		 */
		public abstract T doPerform() throws KeeperException, InterruptedException;

		public T perform() throws KeeperException, InterruptedException {
			awaitConnectionInitialization(backoffStrategyFactory.get());

			T result = null;
			boolean success = false;
			while (!success && !shutdown) {
				try {
					result = doPerform();
					success = true;
				} catch (KeeperException e) {
					handleKeeperException(_backoffStrategy, e);
				}// try
			}// while

			if (shutdown) {
				throw new KeeperException.SessionExpiredException();
			}

			return result;
		}

	}// class

	/**
	 * 
	 * @author ypai
	 * 
	 * @param <T>
	 */
	public abstract class VoidZooKeeperAction {

		/** backoff strategy to use on reconnection attempts */
		private final BackoffStrategy _backoffStrategy;

		public VoidZooKeeperAction(BackoffStrategy _backoffStrategy) {
			this._backoffStrategy = _backoffStrategy;
		}

		/**
		 * 
		 * @return
		 * @throws KeeperException
		 */
		public abstract void doPerform() throws KeeperException, InterruptedException;

		public void perform() throws KeeperException, InterruptedException {
			awaitConnectionInitialization(backoffStrategyFactory.get());

			boolean success = false;
			while (!success && !shutdown) {
				try {
					doPerform();
					success = true;
				} catch (KeeperException e) {
					handleKeeperException(_backoffStrategy, e);
				}// try
			}// while

			if (shutdown) {
				throw new KeeperException.SessionExpiredException();
			}
		}

	}// class

	// public static void main(String[] args) throws Exception {
	// List<ACL> aclList = new ArrayList<ACL>();
	// aclList.add(new ACL(ZooDefs.Perms.ALL, new Id("world", "anyone")));
	//
	// ZooKeeper zk = new ZooKeeper("localhost", 5000, new Watcher() {
	// @Override
	// public void process(WatchedEvent event) {
	// System.out.println("***** Received ZooKeeper Event:  "
	// + ReflectionToStringBuilder.toString(event, ToStringStyle.DEFAULT_STYLE));
	//
	// }
	// });
	//
	// Stat stat = zk.exists("/yentest1234", true);
	// System.out.println("***** Received ZooKeeper Event:  "
	// + ReflectionToStringBuilder.toString(stat, ToStringStyle.DEFAULT_STYLE));
	//
	// zk.create("/yentest1234", null, aclList, CreateMode.EPHEMERAL);
	//
	// Thread.sleep(10000);
	// zk.delete("/yentest1234", -1);
	//
	// }
}
