package org.kompany.overlord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.Watcher;
import org.kompany.overlord.zookeeper.ResilientZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point into framework functionality.
 * 
 * @author ypai
 * 
 */
public class Sovereign {

    private static final Logger logger = LoggerFactory.getLogger(Sovereign.class);

    private ZkClient zkClient;

    private Map<String, ServiceWrapper> serviceMap = new HashMap<String, ServiceWrapper>();

    private PathScheme pathScheme = new DefaultPathScheme();

    private ScheduledExecutorService executorService;

    private volatile boolean started = false;
    private volatile boolean shutdown = false;

    private int threadPoolSize = 3;

    public Sovereign(String zkConnectString, int sessionTimeoutMillis) {
        try {
            zkClient = new ResilientZooKeeper(zkConnectString, sessionTimeoutMillis);
        } catch (IOException e) {
            throw new IllegalStateException("Fatal error:  could not initialize Zookeeper client!");
        }
    }

    public ZkClient getZkClient() {
        return zkClient;
    }

    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    public PathScheme getPathScheme() {
        return pathScheme;
    }

    public void setPathScheme(PathScheme pathScheme) {
        this.pathScheme = pathScheme;
    }

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    public void setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

    public boolean isStarted() {
        return started;
    }

    public boolean isShutdown() {
        return shutdown;
    }

    public Service getService(String serviceName) {
        if (started) {
            throw new IllegalStateException("Cannot use a service before starting!");
        }

        ServiceWrapper serviceWrapper = this.serviceMap.get(serviceName);
        return serviceWrapper.getService();
    }

    public synchronized void register(String serviceName, Service service) {
        if (started) {
            throw new IllegalStateException("Cannot register services once started!");
        }
        if (zkClient == null) {
            throw new IllegalStateException("Cannot register services before ZooKeeper client has been populated!");
        }

        // set with path scheme
        service.setPathScheme(pathScheme);
        service.setZkClient(zkClient);
        service.init();

        // add to zkClient's list of watchers if Watcher interface is
        // implemented
        if (service instanceof Watcher) {
            logger.debug("zkClient={}", zkClient);
            zkClient.register((Watcher) service);
        }

        serviceMap.put(service.getClass().getSimpleName(), new ServiceWrapper(service));
    }

    public synchronized void registerServices(Map<String, Service> serviceMap) {
        for (String serviceName : serviceMap.keySet()) {
            register(serviceName, serviceMap.get(serviceName));
        }
    }

    public synchronized void start() {
        if (started) {
            return;
        }
        started = true;

        /** init executor **/
        this.executorService = this.createExecutorService();

        /** start services running **/
        for (ServiceWrapper serviceWrapper : serviceMap.values()) {
            logger.debug("Checking service:  {}", serviceWrapper.getService().getClass().getName());

            // execute if not a continuously running service and not shutdown
            if (!isShutdown() && serviceWrapper.isSubmittable()) {
                logger.debug("Submitting service:  {}", serviceWrapper.getService().getClass().getName());
                executorService.scheduleWithFixedDelay(serviceWrapper, 0, serviceWrapper.getIntervalMillis(),
                        TimeUnit.MILLISECONDS);
            }// if
        }// for

        /** start main thread **/
        // Thread mainThread = this.createMainThread();
        // mainThread.start();
    }

    public synchronized void stop() {
        if (shutdown) {
            return;
        }
        shutdown = true;

        executorService.shutdown();

        for (ServiceWrapper serviceWrapper : serviceMap.values()) {
            serviceWrapper.getService().destroy();
        }
    }

    /**
     * Creates and appropriately sizes executor service for services configured.
     * 
     * @return
     */
    private ScheduledExecutorService createExecutorService() {
        // get number of continuously running services
        int continuouslyRunningCount = 0;
        for (ServiceWrapper serviceWrapper : serviceMap.values()) {
            if (serviceWrapper.isActiveService() && serviceWrapper.isAlwaysActive()) {
                continuouslyRunningCount++;
            }
        }
        return new ScheduledThreadPoolExecutor(continuouslyRunningCount + this.getThreadPoolSize());
    }

    /**
     * Creates the thread that submits the services for regular execution.
     * 
     * @return
     */
    private Thread createMainThread() {
        final ScheduledExecutorService finalExecutorService = this.executorService;
        Thread mainThread = new Thread() {
            @Override
            public void run() {
                while (true) {
                    // TODO: perform any framework maintenance?

                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        logger.warn("Interrupted while in mainThread:  " + e, e);
                    }
                }// while
            }// run()
        };
        mainThread.setName(this.getClass().getSimpleName() + "." + "main");
        mainThread.setDaemon(true);
        return mainThread;
    }

    /**
     * Wrapper providing additional properties that allow us to track service execution metadata.
     * 
     * @author ypai
     * 
     */
    private static class ServiceWrapper implements Runnable {
        private Service service;
        private boolean running = false;

        public ServiceWrapper(Service service) {
            this.service = service;
        }

        public Service getService() {
            return service;
        }

        public boolean isActiveService() {
            return service instanceof ActiveService;
        }

        public boolean isSubmittable() {
            return isActiveService() && !isRunning();
        }

        public synchronized boolean isAlwaysActive() {
            return ((ActiveService) this.service).getExecutionIntervalMillis() < 1;
        }

        public synchronized boolean isRunning() {
            return running;
        }

        public long getIntervalMillis() {
            return ((ActiveService) service).getExecutionIntervalMillis();
        }

        @Override
        public synchronized void run() {
            this.running = true;

            logger.debug("Calling {}.perform()", service.getClass().getName());
            ((ActiveService) this.service).perform();

            this.running = false;
        }

    }
}
