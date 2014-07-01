/*
 * Copyright 2013 Yen Pai ypai@kompany.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.reign;

import io.reign.presence.PresenceService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point into framework functionality.
 * 
 * @author ypai
 * 
 */
public class Reign implements Watcher {

    private static final Logger logger = LoggerFactory.getLogger(Reign.class);

    public static final String DEFAULT_FRAMEWORK_CLUSTER_ID = "reign";
    public static final String CLIENT_SERVICE_ID = "client";

    public static final String DEFAULT_FRAMEWORK_BASE_PATH = "/reign";

    public static final List<ACL> DEFAULT_ACL_LIST = new ArrayList<ACL>();
    static {
        DEFAULT_ACL_LIST.add(new ACL(ZooDefs.Perms.ALL, new Id("world", "anyone")));
    }

    public static int DEFAULT_MESSAGING_PORT = 33033;

    private ZkClient zkClient;

    private final Map<String, ServiceWrapper> serviceMap = new ConcurrentHashMap<String, ServiceWrapper>(8, 0.9f, 2);

    private ReignContext context;

    // private final Map<String, Future<?>> futureMap = new HashMap<String, Future<?>>();

    private PathScheme pathScheme;

    private volatile boolean started = false;
    private volatile boolean shutdown = false;

    private int threadPoolSize = 3;

    /** List to ensure Watcher(s) are called in a specific order */
    private final List<Watcher> watcherList = new ArrayList<Watcher>();

    private List<ACL> defaultZkAclList = DEFAULT_ACL_LIST;

    private NodeIdProvider nodeIdProvider;

    private final ObserverManager observerManager;

    private TestingServer zkTestServer;

    public static ReignMaker maker() {
        return new ReignMaker();
    }

    // public Reign() {
    // }

    public Reign(ZkClient zkClient, PathScheme pathScheme, NodeIdProvider nodeIdProvider, TestingServer zkTestServer) {

        this.zkClient = zkClient;

        this.pathScheme = pathScheme;

        this.nodeIdProvider = nodeIdProvider;

        observerManager = new ObserverManager(zkClient);

        this.zkTestServer = zkTestServer;

    }

    public synchronized NodeIdProvider getCanonicalIdProvider() {
        if (!started) {
            throw new IllegalStateException("Cannot get provider before framework is started!");
        }
        return this.nodeIdProvider;
    }

    public synchronized ReignContext getContext() {
        if (!started) {
            throw new IllegalStateException("Cannot get context before framework is started!");
        }
        return this.context;
    }

    public List<ACL> getDefaultZkAclList() {
        return defaultZkAclList;
    }

    public synchronized void setDefaultZkAclList(List<ACL> defaultZkAclList) {
        if (started) {
            throw new IllegalStateException("Cannot set defaultAclList once started!");
        }
        this.defaultZkAclList = defaultZkAclList;
    }

    public synchronized void setCanonicalIdProvider(NodeIdProvider canonicalIdMaker) {
        if (started) {
            throw new IllegalStateException("Cannot set canonicalIdMaker once started!");
        }
        this.nodeIdProvider = canonicalIdMaker;
    }

    @Override
    public void process(WatchedEvent event) {
        // log if TRACE
        if (logger.isTraceEnabled()) {
            logger.trace("***** Received ZooKeeper Event:  {}",
                    ReflectionToStringBuilder.toString(event, ToStringStyle.DEFAULT_STYLE));

        }

        if (shutdown) {
            logger.warn("Already shutdown:  ignoring event:  type={}; path={}", event.getType(), event.getPath());
            return;
        }

        for (Watcher watcher : watcherList) {
            watcher.process(event);
        }
    }

    public ZkClient getZkClient() {
        return zkClient;
    }

    public synchronized void setZkClient(ZkClient zkClient) {
        if (started) {
            throw new IllegalStateException("Cannot set zkClient once started!");
        }
        this.zkClient = zkClient;
    }

    public PathScheme getPathScheme() {
        return pathScheme;
    }

    public synchronized void setPathScheme(PathScheme pathScheme) {
        if (started) {
            throw new IllegalStateException("Cannot set pathScheme once started!");
        }
        this.pathScheme = pathScheme;
    }

    // public PathCache getPathCache() {
    // return pathCache;
    // }
    //
    // public synchronized void setPathCache(SimplePathCache pathCache) {
    // if (started) {
    // throw new IllegalStateException("Cannot set pathCache once started!");
    // }
    // this.pathCache = pathCache;
    // }

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    public synchronized void setThreadPoolSize(int threadPoolSize) {
        if (started) {
            throw new IllegalStateException("Cannot set threadPoolSize once started!");
        }
        this.threadPoolSize = threadPoolSize;
    }

    public synchronized <T extends Service> T getService(String serviceName) {
        if (!started) {
            throw new IllegalStateException("Cannot get service before framework is started!");
        }
        return context.getService(serviceName);
    }

    void register(String serviceName, Service service) {
        throwExceptionIfNotOkayToRegister();

        logger.info("Registering service:  serviceName={}", serviceName);

        // check that we don't have duplicate services
        if (serviceMap.put(serviceName, new ServiceWrapper(service)) != null) {
            throw new IllegalStateException("An existing service already exists under the same name:  serviceName="
                    + serviceName);
        }

    }

    public synchronized void registerServices(Map<String, Service> serviceMap) {
        throwExceptionIfNotOkayToRegister();

        for (String serviceName : serviceMap.keySet()) {
            register(serviceName, serviceMap.get(serviceName));
        }

    }

    public synchronized void start() {
        if (started) {
            logger.debug("start():  already started...");
            return;
        }

        logger.info("START:  begin");

        /** create graceful shutdown hook **/
        logger.info("START:  add shutdown hook");
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                Reign.this.stop();
            }
        });

        /** init observer manager **/
        observerManager.init();

        // /** init path cache **/
        // logger.info("START:  initializing pathCache...");
        // pathCache.init();

        /** create context object **/
        logger.info("START:  creating ReignContext...");
        final List<ACL> finalDefaultZkAclList = defaultZkAclList;
        this.context = new ReignContext() {

            @Override
            public Service getService(String serviceName) {
                if (shutdown) {
                    throw new IllegalStateException("Already shutdown:  cannot get service.");
                }
                waitForInitializationIfNecessary();

                if (serviceName == null) {
                    return serviceMap.get("null").getService();
                }

                ServiceWrapper serviceWrapper = serviceMap.get(serviceName);
                if (serviceWrapper != null) {
                    return serviceWrapper.getService();
                } else {
                    return null;
                }
            }

            @Override
            public NodeId getNodeId() {
                return nodeIdProvider.get();
            }

            @Override
            public ZkNodeId getZkNodeId() {
                return nodeIdProvider.forZk();
            }

            @Override
            public ZkClient getZkClient() {
                return zkClient;
            }

            @Override
            public PathScheme getPathScheme() {
                return pathScheme;
            }

            @Override
            public List<ACL> getDefaultZkAclList() {
                return finalDefaultZkAclList;
            }

            // @Override
            // public String getCanonicalIdPathToken() {
            // return nodeIdProvider.get().toString();
            // }

            @Override
            public ObserverManager getObserverManager() {
                return observerManager;
            }

            @Override
            public NodeId getNodeIdFromZk(ZkNodeId zkNodeId) {
                return nodeIdProvider.fromZk(zkNodeId);
            }

            // @Override
            // public NodeIdProvider getNodeIdProvider() {
            // return nodeIdProvider;
            // }

        };

        /** init services **/
        for (String serviceName : serviceMap.keySet()) {
            logger.info("START:  initializing:  serviceName={}", serviceName);

            Service service = serviceMap.get(serviceName).getService();
            service.setPathScheme(pathScheme);
            service.setZkClient(zkClient);
            service.setObserverManager(observerManager);
            service.setContext(context);
            service.setDefaultZkAclList(defaultZkAclList);
            service.init();

            // add to zkClient's list of watchers if Watcher interface is
            // implemented
            if (service instanceof Watcher) {
                logger.info("START:  adding as ZooKeeper watcher:  serviceName={}", serviceName);
                watcherList.add((Watcher) service);
            }
        }

        /** watcher set-up **/
        logger.info("START:  registering watchers");
        // register self as watcher
        this.zkClient.register(this);

        // // pathCache should be first in list if it is a Watcher
        // if (pathCache instanceof Watcher) {
        // this.watcherList.add(0, (Watcher) pathCache);
        // }

        // /** init executor **/
        // logger.info("START:  initializing executor");
        // this.executorService = this.createExecutorService();

        // /** start services running **/
        // logger.info("START:  schedule service tasks");
        // for (String serviceName : serviceMap.keySet()) {
        // ServiceWrapper serviceWrapper = serviceMap.get(serviceName);
        // logger.debug("Checking service:  {}", serviceWrapper.getService().getClass().getName());
        //
        // // execute if not a continuously running service and not shutdown
        // if (!this.shutdown && serviceWrapper.isSubmittable()) {
        // logger.debug("Submitting service:  {}", serviceWrapper.getService().getClass().getName());
        // Future<?> future = executorService.scheduleWithFixedDelay(serviceWrapper, serviceWrapper
        // .getIntervalMillis(), serviceWrapper.getIntervalMillis(), TimeUnit.MILLISECONDS);
        // futureMap.put(serviceName, future);
        // }// if
        // }// for

        started = true;

        /** notify any waiters **/
        logger.info("START:  notifying all waiters");
        this.notifyAll();

        logger.info("START:  DONE");

        /** announce as a Reign Server: must be done after all other start-up tasks are complete **/
        PresenceService presenceService = context.getService("presence");
        if (presenceService != null) {
            logger.info("START:  announcing server availability...");
            presenceService.announce(pathScheme.getFrameworkClusterId(), "server", true);
        } else {
            logger.warn("START:  did not announce node availability:  (presenceService==null)={}",
                    presenceService == null);
        }
    }

    public synchronized void stop() {
        if (shutdown) {
            logger.debug("stop():  already stopped...");
            return;
        }
        shutdown = true;

        logger.info("SHUTDOWN:  begin");

        /** clean up services **/
        logger.info("SHUTDOWN:  cleaning up services");
        for (ServiceWrapper serviceWrapper : serviceMap.values()) {
            serviceWrapper.getService().destroy();
        }

        /** observer manager **/
        logger.info("SHUTDOWN:  stopping observer manager");
        observerManager.destroy();

        // /** init path cache **/
        // logger.info("SHUTDOWN:  stopping pathCache...");
        // pathCache.destroy();

        /** clean up zk client **/
        logger.info("SHUTDOWN:  closing Zookeeper client");
        this.zkClient.close();

        /** shutdown test zk server, if there **/
        if (this.zkTestServer != null) {
            logger.info("SHUTDOWN:  stopping test Zookeeper server");
            try {
                this.zkTestServer.stop();
            } catch (IOException e) {
                logger.error("SHUTDOWN:  error shutting down test ZooKeeper:  " + e, e);
            }
        }

        logger.info("SHUTDOWN:  DONE");
    }

    private void throwExceptionIfNotOkayToRegister() {
        if (started) {
            throw new IllegalStateException("Cannot register services once started!");
        }
        if (zkClient == null) {
            throw new IllegalStateException("Cannot register services before zkClient is initialized!");
        }
    }

    private void waitForInitializationIfNecessary() {
        if (!started) {
            try {
                logger.info("Waiting for notification of start() completion...");
                synchronized (Reign.this) {
                    Reign.this.wait(30000);
                }
            } catch (InterruptedException e) {
                logger.warn("Interrupted while waiting for start:  " + e, e);
            }
            if (!started) {
                throw new IllegalStateException("Not yet initialized:  check environment and ZK settings.");
            }
            logger.info("Received notification of start() completion");
        }// if
    }

    /**
     * Convenience wrapper providing methods for interpreting service metadata.
     * 
     * @author ypai
     * 
     */
    private static class ServiceWrapper {
        private final Service service;

        public ServiceWrapper(Service service) {
            this.service = service;
        }

        public Service getService() {
            return service;
        }

    }

}
