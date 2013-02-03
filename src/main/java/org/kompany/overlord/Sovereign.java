package org.kompany.overlord;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.Watcher;
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

    private PathScheme pathScheme;

    private ExecutorService executorService;

    private volatile boolean started = false;
    private volatile boolean shutdown = false;

    private int threadPoolSizeMin = 3;

    private int threadPoolSizeMax = 6;

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

    public int getThreadPoolSizeMin() {
        return threadPoolSizeMin;
    }

    public void setThreadPoolSizeMin(int threadPoolSizeMin) {
        this.threadPoolSizeMin = threadPoolSizeMin;
    }

    public int getThreadPoolSizeMax() {
        return threadPoolSizeMax;
    }

    public void setThreadPoolSizeMax(int threadPoolSizeMax) {
        this.threadPoolSizeMax = threadPoolSizeMax;
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

        // set with path scheme
        service.setPathScheme(pathScheme);
        service.setZkClient(zkClient);
        service.init();

        // add to zkClient's list of watchers if Watcher interface is
        // implemented
        if (service instanceof Watcher) {
            zkClient.register((Watcher) service);
        }

        serviceMap.put(service.getClass().getSimpleName(), new ServiceWrapper(service));
    }

    public synchronized void registerServices(Map<String, Service> serviceMap) {
        if (started) {
            throw new IllegalStateException("Cannot register services once started!");
        }

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

        /** start main thread **/
        Thread mainThread = this.createMainThread();
        mainThread.start();
    }

    public synchronized void destroy() {
        if (shutdown) {
            return;
        }
        shutdown = true;

        executorService.shutdown();

        for (ServiceWrapper serviceWrapper : serviceMap.values()) {
            serviceWrapper.getService().destroy();
        }
    }

    private ExecutorService createExecutorService() {
        // get number of continuously running services
        int continuouslyRunningCount = 0;
        for (ServiceWrapper serviceWrapper : serviceMap.values()) {
            if (serviceWrapper.isActiveService() && serviceWrapper.isAlwaysActive()) {
                continuouslyRunningCount++;
            }
        }
        return new ThreadPoolExecutor(continuouslyRunningCount + this.getThreadPoolSizeMin(), continuouslyRunningCount
                + this.getThreadPoolSizeMax(), 10, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>()) {

            protected void beforeExecute(Thread t, Runnable r) {
                ((ServiceWrapper) r).setRunning(true);
                ((ServiceWrapper) r).setLastStartedTimestampMillis(System.currentTimeMillis());
            }

            protected void afterExecute(Runnable r, Throwable t) {
                ((ServiceWrapper) r).setRunning(false);
                ((ServiceWrapper) r).setLastCompletedTimestampMillis(System.currentTimeMillis());
            }
        };
    }

    private Thread createMainThread() {
        final ExecutorService finalExecutorService = this.executorService;
        Thread mainThread = new Thread() {
            public void run() {
                while (true) {
                    for (ServiceWrapper serviceWrapper : serviceMap.values()) {
                        long currentTimestampMillis = System.currentTimeMillis();

                        // execute if not a continuously running service and not
                        // shutdown
                        if (!isShutdown()) {
                            if (serviceWrapper.isSubmittable(currentTimestampMillis)) {
                                finalExecutorService.submit(serviceWrapper);
                            }
                        }// if
                    }// for

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
     * Wrapper providing additional properties that allow us to track service
     * execution metadata.
     * 
     * @author ypai
     * 
     */
    private static class ServiceWrapper implements Runnable {
        private Service service;
        private volatile long lastStartedTimestampMillis;
        private volatile long lastCompletedTimestampMillis;
        private volatile boolean running;

        public ServiceWrapper(Service service) {
            this.service = service;
        }

        public long getLastStartedTimestampMillis() {
            return lastStartedTimestampMillis;
        }

        public void setLastStartedTimestampMillis(long lastStartedTimestampMillis) {
            this.lastStartedTimestampMillis = lastStartedTimestampMillis;
        }

        public long getLastCompletedTimestampMillis() {
            return lastCompletedTimestampMillis;
        }

        public void setLastCompletedTimestampMillis(long lastCompletedTimestampMillis) {
            this.lastCompletedTimestampMillis = lastCompletedTimestampMillis;
        }

        public boolean isSubmittable(long currentTimestampMillis) {
            return isActiveService()
                    && !running
                    && currentTimestampMillis - lastStartedTimestampMillis > ((ActiveService) this.service)
                            .getExecutionIntervalMillis();
        }

        public void setRunning(boolean running) {
            this.running = running;
        }

        public Service getService() {
            return service;
        }

        public boolean isActiveService() {
            return service instanceof ActiveService;
        }

        public boolean isAlwaysActive() {
            return ((ActiveService) this.service).getExecutionIntervalMillis() < 1;
        }

        @Override
        public void run() {
            ((ActiveService) this.service).perform();
        }

    }
}
