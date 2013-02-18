package org.kompany.overlord.zookeeper;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

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
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.kompany.overlord.ZkClient;
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
public class ResilientZooKeeper implements ZkClient, Watcher {

    private static final Logger logger = LoggerFactory.getLogger(ResilientZooKeeper.class);

    private static final BackoffStrategy DEFAULT_BACKOFF_STRATEGY = new ExponentialBackoffStrategy(2000L, 120000, false);

    private volatile ZooKeeper zooKeeper;

    private volatile boolean connected = false;

    private Set<Watcher> watcherSet = new HashSet<Watcher>();

    private String connectString;

    private int sessionTimeoutMillis;

    private static long ASSUME_ERROR_TIMEOUT_MS = 60000;

    /** when true, we do not attempt reconnect on failure */
    private volatile boolean shutdown = false;

    /** backoff strategy */
    private BackoffStrategy defaultBackoffStrategy = DEFAULT_BACKOFF_STRATEGY;

    /** object to synchronize on for connection/re-connection attempts */
    private ReentrantLock connectionLock = new ReentrantLock();

    public ResilientZooKeeper(String connectString, int sessionTimeoutMillis, long sessionId, byte[] sessionPasswd)
            throws IOException {
        this.connectString = connectString;
        this.sessionTimeoutMillis = sessionTimeoutMillis;
        this.zooKeeper = new ZooKeeper(connectString, sessionTimeoutMillis, this, sessionId, sessionPasswd);
    }

    public ResilientZooKeeper(String connectString, int sessionTimeoutMillis) throws IOException {
        this.connectString = connectString;
        this.sessionTimeoutMillis = sessionTimeoutMillis;
        this.zooKeeper = new ZooKeeper(connectString, sessionTimeoutMillis, this);
    }

    public BackoffStrategy getDefaultBackoffStrategy() {
        return defaultBackoffStrategy;
    }

    public void setDefaultBackoffStrategy(BackoffStrategy defaultBackoffStrategy) {
        this.defaultBackoffStrategy = defaultBackoffStrategy;
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

    @Override
    public void close() {
        this.connectionLock.lock();
        try {
            this.shutdown = true;

            if (this.zooKeeper != null) {
                try {
                    if (logger.isInfoEnabled()) {
                        logger.info("Closing ZooKeeper session:  connectString={}", getConnectString());
                    }
                    this.zooKeeper.close();
                    this.connected = false;
                } catch (InterruptedException e) {
                    logger.warn("Sleep interrupted while closing existing ZooKeeper session:  " + e, e);
                } // try
            }// if
        } finally {
            this.connectionLock.unlock();
        }
    }

    public void create(final String path, byte[] data, List<ACL> acl, CreateMode createMode, final StringCallback cb,
            Object ctx) {

        awaitConnectionInitialization();

        // create a wrapper callback to handle connection errors
        StringCallback wrapperCallback = new StringCallback() {
            @Override
            public void processResult(int __rc, String __path, Object __ctx, String __name) {

                while (!shutdown) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("create():  path={}; zooKeeper={}", path, zooKeeper);
                    }

                    KeeperException.Code keeperExceptionCode = KeeperException.Code.get(__rc);
                    if (keeperExceptionCode != KeeperException.Code.NODEEXISTS
                            && keeperExceptionCode != KeeperException.Code.NONODE) {
                        logger.error("create():  KeeperException.Code={}", keeperExceptionCode);
                    }

                    if (!defaultBackoffStrategy.hasNext()) {
                        // failfast mode, so we call the callback and allow
                        // caller to handle error
                        cb.processResult(__rc, __path, __ctx, __name);
                        break;
                    } else if (isZooKeeperSessionError(keeperExceptionCode)) {
                        // if it is a ZK session error, wait for connection to
                        // be re-established
                        awaitSessionRenewal(defaultBackoffStrategy, keeperExceptionCode);

                    } else {
                        // not ZK session error, so call original callback
                        cb.processResult(__rc, __path, __ctx, __name);
                        break;
                    }// if
                }// while

            }// processResult
        };

        // call the async method
        this.zooKeeper.create(path, data, acl, createMode, wrapperCallback, ctx);
    }

    public void delete(final String path, int version, final VoidCallback cb, final Object ctx) {
        awaitConnectionInitialization();

        // create a wrapper callback to handle connection errors
        VoidCallback wrapperCallback = new VoidCallback() {
            @Override
            public void processResult(int __rc, String __path, Object __ctx) {

                while (!shutdown) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("create():  path={}; zooKeeper={}", path, zooKeeper);
                    }

                    KeeperException.Code keeperExceptionCode = KeeperException.Code.get(__rc);
                    if (keeperExceptionCode != KeeperException.Code.NODEEXISTS
                            && keeperExceptionCode != KeeperException.Code.NONODE) {
                        logger.error("create():  KeeperException.Code={}", keeperExceptionCode);
                    }

                    if (!defaultBackoffStrategy.hasNext()) {
                        // failfast mode, so we call the callback and allow
                        // caller to handle error
                        cb.processResult(__rc, path, ctx);
                        break;
                    } else if (isZooKeeperSessionError(keeperExceptionCode)) {
                        // if it is a ZK session error, wait for connection to
                        // be re-established
                        awaitSessionRenewal(defaultBackoffStrategy, keeperExceptionCode);
                    } else {
                        // not ZK session error, so call original callback
                        cb.processResult(__rc, path, ctx);
                        break;
                    }// if
                }// while

            }// processResult
        };

        // call the async method
        this.zooKeeper.delete(path, version, wrapperCallback, ctx);
    }

    /**
     * TODO: make tolerant of ZK connection failures
     */
    public void exists(String path, boolean watch, StatCallback cb, Object ctx) {
        this.zooKeeper.exists(path, watch, cb, ctx);
    }

    /**
     * TODO: make tolerant of ZK connection failures
     */
    public void exists(String path, Watcher watcher, StatCallback cb, Object ctx) {
        this.zooKeeper.exists(path, watcher, cb, ctx);
    }

    /**
     * TODO: make tolerant of ZK connection failures
     */
    public void getACL(String path, Stat stat, ACLCallback cb, Object ctx) {
        this.zooKeeper.getACL(path, stat, cb, ctx);
    }

    public List<ACL> getACL(final String path, final Stat stat) throws KeeperException, InterruptedException {

        ZooKeeperAction<List<ACL>> zkAction = new ZooKeeperAction<List<ACL>>(defaultBackoffStrategy) {
            @Override
            public List<ACL> doPerform() throws KeeperException, InterruptedException {
                return zooKeeper.getACL(path, stat);

            }
        };

        return zkAction.perform();
    }

    /**
     * TODO: make tolerant of ZK connection failures
     */
    public void getChildren(String path, boolean watch, Children2Callback cb, Object ctx) {
        this.zooKeeper.getChildren(path, watch, cb, ctx);
    }

    /**
     * TODO: make tolerant of ZK connection failures
     */
    public void getChildren(String path, boolean watch, ChildrenCallback cb, Object ctx) {
        this.zooKeeper.getChildren(path, watch, cb, ctx);
    }

    @Override
    public List<String> getChildren(final String path, final boolean watch, final Stat stat) throws KeeperException,
            InterruptedException {

        ZooKeeperAction<List<String>> zkAction = new ZooKeeperAction<List<String>>(defaultBackoffStrategy) {
            @Override
            public List<String> doPerform() throws KeeperException, InterruptedException {
                return zooKeeper.getChildren(path, watch, stat);

            }
        };

        return zkAction.perform();
    }

    /**
     * TODO: make tolerant of ZK connection failures
     */
    public void getChildren(String path, Watcher watcher, Children2Callback cb, Object ctx) {
        this.zooKeeper.getChildren(path, watcher, cb, ctx);
    }

    /**
     * TODO: make tolerant of ZK connection failures
     */
    public void getChildren(String path, Watcher watcher, ChildrenCallback cb, Object ctx) {
        this.zooKeeper.getChildren(path, watcher, cb, ctx);
    }

    public List<String> getChildren(final String path, final Watcher watcher, final Stat stat) throws KeeperException,
            InterruptedException {

        ZooKeeperAction<List<String>> zkAction = new ZooKeeperAction<List<String>>(defaultBackoffStrategy) {
            @Override
            public List<String> doPerform() throws KeeperException, InterruptedException {
                return zooKeeper.getChildren(path, watcher, stat);

            }
        };

        return zkAction.perform();
    }

    public List<String> getChildren(final String path, final Watcher watcher) throws KeeperException,
            InterruptedException {

        ZooKeeperAction<List<String>> zkAction = new ZooKeeperAction<List<String>>(defaultBackoffStrategy) {
            @Override
            public List<String> doPerform() throws KeeperException, InterruptedException {
                return zooKeeper.getChildren(path, watcher);

            }
        };

        return zkAction.perform();
    }

    /**
     * TODO: make tolerant of ZK connection failures
     */
    public void getData(String path, boolean watch, DataCallback cb, Object ctx) {
        this.zooKeeper.getData(path, watch, cb, ctx);
    }

    /**
     * TODO: make tolerant of ZK connection failures
     */
    public void getData(String path, Watcher watcher, DataCallback cb, Object ctx) {
        this.zooKeeper.getData(path, watcher, cb, ctx);
    }

    public byte[] getData(final String path, final Watcher watcher, final Stat stat) throws KeeperException,
            InterruptedException {

        ZooKeeperAction<byte[]> zkAction = new ZooKeeperAction<byte[]>(defaultBackoffStrategy) {
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
    }

    public void setACL(final String path, final List<ACL> acl, final int version, final StatCallback cb,
            final Object ctx) {

        VoidZooKeeperAction zkAction = new VoidZooKeeperAction(defaultBackoffStrategy) {
            @Override
            public void doPerform() throws KeeperException, InterruptedException {
                zooKeeper.setACL(path, acl, version, cb, ctx);

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

    public Stat setACL(final String path, final List<ACL> acl, final int version) throws KeeperException,
            InterruptedException {

        ZooKeeperAction<Stat> zkAction = new ZooKeeperAction<Stat>(defaultBackoffStrategy) {
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
        VoidZooKeeperAction zkAction = new VoidZooKeeperAction(defaultBackoffStrategy) {
            @Override
            public void doPerform() throws KeeperException, InterruptedException {
                zooKeeper.setData(path, data, version, cb, ctx);

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
     * @param cb
     * @param ctx
     */
    public void sync(final String path, final VoidCallback cb, final Object ctx) {
        VoidZooKeeperAction zkAction = new VoidZooKeeperAction(defaultBackoffStrategy) {

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

        ZooKeeperAction<String> zkAction = new ZooKeeperAction<String>(defaultBackoffStrategy) {

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

        // awaitConnectionInitialization();
        //
        // String pathCreated = null;
        //
        // while (pathCreated == null && !this.shutdown) {
        // if (logger.isDebugEnabled()) {
        // logger.debug("create():  path=" + path + "; zooKeeper="
        // + zooKeeper);
        // }
        //
        // try {
        // pathCreated = this.zooKeeper
        // .create(path, data, acl, createMode);
        //
        // } catch (KeeperException e) {
        // KeeperException.Code keeperExceptionCode = e.code();
        // if (keeperExceptionCode != KeeperException.Code.NODEEXISTS
        // && keeperExceptionCode != KeeperException.Code.NONODE) {
        // logger.error("create():  " + e, e);
        // }
        //
        // if (!defaultBackoffStrategy.hasNext()) {
        // // just throw exception if in fail fast mode
        // throw e;
        // } else if (isZooKeeperSessionError(e.code())) {
        // // if it is a ZK session error, wait for connection to be
        // // re-established
        // awaitSessionRenewal(defaultBackoffStrategy, e.code());
        // } else {
        // throw e;
        // }// if
        // }// try
        // }// while
        //
        // if (logger.isDebugEnabled()) {
        // logger.debug("create():  Path created:  pathCreated={}",
        // pathCreated);
        // }
        //
        // return pathCreated;
    }

    /**
     * 
     * @return
     */
    public ZooKeeper.States getState() {

        ZooKeeperAction<ZooKeeper.States> zkAction = new ZooKeeperAction<ZooKeeper.States>(defaultBackoffStrategy) {
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

        // awaitConnectionInitialization();
        //
        // ZooKeeper.States state = null;
        // if (!this.shutdown) {
        // try {
        // state = this.zooKeeper.getState();
        //
        // } catch (Exception e) {
        // logger.error("" + e, e);
        //
        // // commenting out for now
        // // state = ZooKeeper.States.CLOSED;
        // }// try
        // }// if
        //
        // return state;
    }

    /**
     * 
     * @param path
     * @param watch
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public Stat exists(final String path, final boolean watch) throws KeeperException, InterruptedException {

        ZooKeeperAction<Stat> zkAction = new ZooKeeperAction<Stat>(defaultBackoffStrategy) {

            @Override
            public Stat doPerform() throws KeeperException, InterruptedException {
                return zooKeeper.exists(path, watch);
            }

        };
        return zkAction.perform();

        // awaitConnectionInitialization();
        //
        // Stat stat = null;
        // boolean success = false;
        // while (!success && !this.shutdown) {
        // try {
        // stat = this.zooKeeper.exists(path, watch);
        // success = true;
        // } catch (KeeperException e) {
        // // logger.error("" + e, e);
        //
        // if (!defaultBackoffStrategy.hasNext()) {
        // // just throw exception if in fail fast mode
        // throw e;
        // } else if (this.isZooKeeperSessionError(e.code())) {
        // // if it is a ZK session error, wait for connection to be
        // // re-established
        // awaitSessionRenewal(defaultBackoffStrategy, e.code());
        // } else {
        // throw e;
        // }// if
        // }// try
        // }// while
        //
        // return stat;
    }

    /**
     * 
     * @param path
     * @param watch
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public List<String> getChildren(final String path, final boolean watch) throws KeeperException,
            InterruptedException {

        ZooKeeperAction<List<String>> zkAction = new ZooKeeperAction<List<String>>(defaultBackoffStrategy) {

            @Override
            public List<String> doPerform() throws KeeperException, InterruptedException {
                return zooKeeper.getChildren(path, watch);
            }

        };
        return zkAction.perform();

        // awaitConnectionInitialization();
        //
        // List<String> childList = null;
        // boolean success = false;
        // while (!success && !this.shutdown) {
        // try {
        // childList = this.zooKeeper.getChildren(path, watch);
        // success = true;
        // } catch (KeeperException e) {
        // // logger.error("" + e, e);
        //
        // handleKeeperException(defaultBackoffStrategy, e);
        // }// try
        // }// while
        //
        // return childList;
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

        VoidZooKeeperAction zkAction = new VoidZooKeeperAction(defaultBackoffStrategy) {

            @Override
            public void doPerform() throws KeeperException, InterruptedException {
                zooKeeper.delete(path, version);
            }

        };
        zkAction.perform();

        // awaitConnectionInitialization();
        //
        // while (!this.shutdown) {
        // try {
        // this.zooKeeper.delete(path, version);
        // break;
        // } catch (KeeperException e) {
        // // logger.error("" + e, e);
        //
        // if (isFailFast()) {
        // // just throw exception if in fail fast mode
        // throw e;
        // } else if (this.isZooKeeperSessionError(e)) {
        // // if it is a ZK session error, wait for connection to be
        // // re-established
        // awaitSessionRenewal();
        // } else {
        // throw e;
        // }// if
        // }// try
        // }// while

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

        ZooKeeperAction<Stat> zkAction = new ZooKeeperAction<Stat>(defaultBackoffStrategy) {

            @Override
            public Stat doPerform() throws KeeperException, InterruptedException {
                Stat stat = zooKeeper.setData(path, data, version);
                return stat;
            }

        };
        return zkAction.perform();

        // awaitConnectionInitialization();
        //
        // Stat stat = null;
        // boolean success = false;
        // while (!success && !this.shutdown) {
        // try {
        // stat = this.zooKeeper.setData(path, data, version);
        // success = true;
        // } catch (KeeperException e) {
        // // logger.error("" + e, e);
        //
        // if (isFailFast()) {
        // // just throw exception if in fail fast mode
        // throw e;
        // } else if (this.isZooKeeperSessionError(e)) {
        // // if it is a ZK session error, wait for connection to be
        // // re-established
        // awaitSessionRenewal();
        // } else {
        // throw e;
        // }// if
        // }// try
        // }// while
        //
        // return stat;
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

        ZooKeeperAction<byte[]> zkAction = new ZooKeeperAction<byte[]>(defaultBackoffStrategy) {

            @Override
            public byte[] doPerform() throws KeeperException, InterruptedException {
                byte[] data = zooKeeper.getData(path, watch, stat);
                return data;
            }

        };
        return zkAction.perform();

        // awaitConnectionInitialization();
        //
        // byte[] data = null;
        // boolean success = false;
        // while (!success && !this.shutdown) {
        // try {
        // data = this.zooKeeper.getData(path, watch, stat);
        // success = true;
        // } catch (KeeperException e) {
        // // logger.error("" + e, e);
        //
        // if (isFailFast()) {
        // // just throw exception if in fail fast mode
        // throw e;
        // } else if (this.isZooKeeperSessionError(e)) {
        // // if it is a ZK session error, wait for connection to be
        // // re-established
        // awaitSessionRenewal();
        // } else {
        // throw e;
        // }// if
        // }// try
        // }// while
        //
        // return data;
    }

    /**
     * 
     * @param path
     * @param watcher
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public Stat exists(final String path, final Watcher watcher) throws KeeperException, InterruptedException {
        ZooKeeperAction<Stat> zkAction = new ZooKeeperAction<Stat>(defaultBackoffStrategy) {

            @Override
            public Stat doPerform() throws KeeperException, InterruptedException {
                Stat stat = zooKeeper.exists(path, watcher);
                return stat;
            }

        };
        return zkAction.perform();

        // awaitConnectionInitialization();
        //
        // Stat stat = null;
        // boolean success = false;
        // while (!success && !this.shutdown) {
        // try {
        // stat = this.zooKeeper.exists(path, watcher);
        // success = true;
        // } catch (KeeperException e) {
        // // logger.error("" + e, e);
        //
        // if (isFailFast()) {
        // // just throw exception if in fail fast mode
        // throw e;
        // } else if (this.isZooKeeperSessionError(e)) {
        // // if it is a ZK session error, wait for connection to be
        // // re-established
        // awaitSessionRenewal();
        // } else {
        // throw e;
        // }// if
        // }// try
        // }// while
        //
        // return stat;
    }

    /**
     * 
     */
    protected boolean connect(BackoffStrategy backoffStrategy, boolean force) {
        /**
         * explicitly set connected flag to false so we will close current connection and attempt reconnect
         **/
        if (force) {
            this.connected = false;
        }

        /** attempt reconnection if necessary **/
        connectionLock.lock();
        try {
            while (!connected && !this.shutdown) {

                // close existing ZK connection if necessary
                if (this.zooKeeper != null) {
                    try {
                        if (logger.isInfoEnabled()) {
                            logger.info("Closing ZooKeeper session:  connectString=" + getConnectString());
                        }
                        this.zooKeeper.close();
                    } catch (InterruptedException e) {
                        logger.warn("Sleep interrupted while closing existing ZooKeeper session:  " + e, e);
                    } // try
                }

                // reconnect to ZK
                try {
                    if (logger.isInfoEnabled()) {
                        logger.info("Connecting to ZooKeeper:  connectString=" + getConnectString());
                    }
                    this.zooKeeper = new ZooKeeper(getConnectString(), getSessionTimeout(), this);

                    long waitStartTimestamp = System.currentTimeMillis();

                    if (logger.isInfoEnabled()) {
                        logger.info("Waiting for establishment of ZooKeeper session:  connectString={}",
                                getConnectString());
                    }

                    // wait to be notified that the connection was established
                    synchronized (this) {
                        this.wait(ASSUME_ERROR_TIMEOUT_MS + 10000);
                    }

                    // check to see if we waited too long to connect
                    if (System.currentTimeMillis() - waitStartTimestamp < ASSUME_ERROR_TIMEOUT_MS) {
                        this.connected = true;

                        if (logger.isInfoEnabled()) {
                            logger.info("ZooKeeper session established:  connectString=" + getConnectString()
                                    + "; sessionId=" + this.zooKeeper.getSessionId() + "; sessionTimeout="
                                    + getSessionTimeout());
                        }
                    } else {
                        // if we waited too long, assume something went wrong
                        // and try to connect again after closing current
                        // connection
                        if (logger.isWarnEnabled()) {
                            logger.warn("Took too long to reconnect:  assuming error and will try to reconnect again.");
                        }
                    }
                } catch (InterruptedException e) {
                    if (logger.isWarnEnabled()) {
                        logger.info("Sleep interrupted while waiting for ZooKeeper session establishment:  connectString="
                                + getConnectString());
                    }
                } catch (Exception e) {
                    logger.error("Could not reconnect to ZooKeeper (retrying in " + backoffStrategy.get() + " ms):  "
                            + e, e);
                    try {
                        Thread.sleep(backoffStrategy.get());
                    } catch (InterruptedException e1) {
                        logger.warn("Sleep interrupted while waiting to reconnect to ZooKeeper:  " + e, e);
                    }

                    if (backoffStrategy.next() == null) {
                        break;
                    }
                }// try
            }// while

            /**
             * return current value of connected: in effect tells us whether reconnection was successful
             **/
            return this.connected;
        } finally {
            connectionLock.unlock();
        }

    } // connect()

    @Override
    public void process(WatchedEvent event) {
        /***** log event *****/
        // log if DEBUG
        if (logger.isDebugEnabled()) {
            logger.debug("***** Received ZooKeeper Event:  {}",
                    ReflectionToStringBuilder.toString(event, ToStringStyle.DEFAULT_STYLE));

        }

        /***** pass event on to registered Watchers *****/
        if (event.getType() != EventType.None) {
            for (Watcher watcher : watcherSet) {
                watcher.process(event);
            }
        }

        /***** process events *****/
        switch (event.getType()) {
        case NodeChildrenChanged:
        case NodeCreated:
        case NodeDataChanged:
        case NodeDeleted:
            break;
        case None:
            Event.KeeperState eventState = event.getState();
            if (eventState == Event.KeeperState.SyncConnected) {
                logger.info("SyncConnected:  notifying all waiters...");
                synchronized (this) {
                    // notify waiting threads that connection has been
                    // established
                    this.notifyAll();
                }
                logger.info("SyncConnected:  notified all waiters");
            } else if (eventState == Event.KeeperState.Disconnected || eventState == Event.KeeperState.Expired) {
                // disconnected; close ZK connection and reconnect
                if (!this.shutdown) {
                    logger.info("Attempting reconnection...");
                    connect(defaultBackoffStrategy, true);
                } else {
                    // should not get here but notify waiting threads
                    logger.info("Disconnected:  notifying all waiters...");
                    synchronized (this) {
                        this.notifyAll();
                    }
                    logger.info("Disconnected:  notified all waiters");
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
    private void awaitConnectionInitialization() {
        while (zooKeeper == null) {
            try {
                logger.debug("Waiting for ZooKeeper initialization...");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.info("Interrupted waiting for ZooKeeper initialization...");
            }
        }
    }

    /**
     * 
     */
    private void awaitSessionRenewal(BackoffStrategy backoffStrategy, KeeperException.Code keeperExceptionCode) {
        // see if we should failfast
        if (!backoffStrategy.hasNext()) {
            // do nothing when failing fast, we let users of this method above
            // deal with any session errors
            return;
        }

        if (logger.isWarnEnabled()) {
            logger.warn("Waiting for session to be re-established...");
        }

        long waitStartTimestamp = System.currentTimeMillis();
        synchronized (connectionLock) {
            try {
                this.wait(ASSUME_ERROR_TIMEOUT_MS + 10000);
            } catch (InterruptedException e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Interrupted while waiting for session to be re-established:  " + e, e);
                }
            }// try
        }// synchronized

        // took too long, so assume error, force reconnection
        if (System.currentTimeMillis() - waitStartTimestamp > ASSUME_ERROR_TIMEOUT_MS) {
            logger.info("Took too long to re-establish session:  attempting to reconnect...");
            connect(backoffStrategy, true);
        } else {
            logger.info("Session re-established.");
        }

    }

    // /**
    // *
    // * @param backoffStrategy
    // * @param keeperExceptionCode
    // * @throws KeeperException
    // */
    // private void handleKeeperException(BackoffStrategy backoffStrategy,
    // KeeperException.Code keeperExceptionCode)
    // throws KeeperException {
    // handleKeeperException(backoffStrategy,
    // KeeperException.create(keeperExceptionCode));
    // }

    /**
     * 
     * @param backoffStrategy
     * @param e
     * @throws KeeperException
     */
    private void handleKeeperException(BackoffStrategy backoffStrategy, KeeperException e) throws KeeperException {
        if (!backoffStrategy.hasNext()) {
            // just throw exception if in fail fast mode
            throw e;
        } else if (this.isZooKeeperSessionError(e.code())) {
            // if it is a ZK session error, wait for connection to be
            // re-established
            awaitSessionRenewal(backoffStrategy, e.code());
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
        private BackoffStrategy _backoffStrategy;

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
            awaitConnectionInitialization();

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
        private BackoffStrategy _backoffStrategy;

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
            awaitConnectionInitialization();

            boolean success = false;
            while (!success && !shutdown) {
                try {
                    doPerform();
                    success = true;
                } catch (KeeperException e) {
                    handleKeeperException(_backoffStrategy, e);
                }// try
            }// while
        }

    }// class

}
