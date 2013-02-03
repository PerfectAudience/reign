package org.kompany.overlord;

import java.util.ArrayList;
import java.util.List;
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
public class Overlord {

    private static final Logger logger = LoggerFactory.getLogger(Overlord.class);

    private ZkClient zkClient;

    private List<PluginWrapper> pluginList = new ArrayList<PluginWrapper>();

    private PathScheme pathScheme;

    private ExecutorService executorService;

    private volatile boolean started = false;

    private int pluginThreadPoolSizeMin = 3;

    private int pluginThreadPoolSizeMax = 6;

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

    public int getPluginThreadPoolSizeMin() {
        return pluginThreadPoolSizeMin;
    }

    public void setPluginThreadPoolSizeMin(int pluginThreadPoolSizeMin) {
        this.pluginThreadPoolSizeMin = pluginThreadPoolSizeMin;
    }

    public int getPluginThreadPoolSizeMax() {
        return pluginThreadPoolSizeMax;
    }

    public void setPluginThreadPoolSizeMax(int pluginThreadPoolSizeMax) {
        this.pluginThreadPoolSizeMax = pluginThreadPoolSizeMax;
    }

    public boolean isStarted() {
        return started;
    }

    /**
     * 
     * @param pluginService
     * @return whether or not the service was initialized correctly.
     */
    public void addPlugin(Plugin plugin) {
        if (plugin.getExecutionIntervalMillis() < 0) {
            logger.warn(
                    "Invalid value for plugin.executionIntervalMillis:  skipping plugin:  plugin.executionIntervalMillis={}",
                    plugin.getExecutionIntervalMillis());
        }

        // set with path scheme
        plugin.setPathScheme(pathScheme);
        plugin.setZkClient(zkClient);
        plugin.init();

        // add to zkClient's list of watchers if Watcher interface is
        // implemented
        if (plugin instanceof Watcher) {
            zkClient.register((Watcher) plugin);
        }

        pluginList.add(new PluginWrapper(plugin));
    }

    /**
     * 
     * @param pluginService
     * @return whether or not the service was initialized correctly.
     */
    public void addPlugins(List<Plugin> pluginList) {
        for (Plugin plugin : pluginList) {
            addPlugin(plugin);
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

    public void destroy() {
        executorService.shutdown();

        for (PluginWrapper pluginWrapper : pluginList) {
            pluginWrapper.getPlugin().destroy();
        }
    }

    private ExecutorService createExecutorService() {
        // get number of continuously running plug-ins
        int continuouslyRunningPlugins = 0;
        for (PluginWrapper pluginWrapper : pluginList) {
            if (pluginWrapper.getPlugin().getExecutionIntervalMillis() == Plugin.EXECUTION_INTERVAL_CONTINUOUS) {
                continuouslyRunningPlugins++;
            }
        }
        return new ThreadPoolExecutor(continuouslyRunningPlugins + pluginThreadPoolSizeMin, continuouslyRunningPlugins
                + pluginThreadPoolSizeMax, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>()) {

            protected void beforeExecute(Thread t, Runnable r) {
                ((PluginWrapper) r).setRunning(true);
                ((PluginWrapper) r).setLastStartedTimestampMillis(System.currentTimeMillis());
            }

            protected void afterExecute(Runnable r, Throwable t) {
                ((PluginWrapper) r).setRunning(false);
                ((PluginWrapper) r).setLastCompletedTimestampMillis(System.currentTimeMillis());
            }
        };
    }

    private Thread createMainThread() {
        final ExecutorService finalExecutorService = this.executorService;
        Thread mainThread = new Thread() {
            public void run() {
                while (true) {
                    for (PluginWrapper pluginWrapper : pluginList) {
                        long currentTimestampMillis = System.currentTimeMillis();
                        Plugin plugin = pluginWrapper.getPlugin();

                        // execute if not a continuously running plug-in and not
                        // shutdown
                        if (!finalExecutorService.isShutdown()
                                && plugin.getExecutionIntervalMillis() > Plugin.EXECUTION_INTERVAL_CONTINUOUS) {
                            if (pluginWrapper.isSubmittable(currentTimestampMillis)) {
                                finalExecutorService.submit(plugin);
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
     * Wrapper providing additional properties that allow us to track plug-in
     * execution.
     * 
     * @author ypai
     * 
     */
    private static class PluginWrapper implements Runnable {
        private Plugin plugin;
        private volatile long lastStartedTimestampMillis;
        private volatile long lastCompletedTimestampMillis;
        private volatile boolean running;

        public PluginWrapper(Plugin plugin) {
            this.plugin = plugin;
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
            return !running
                    && currentTimestampMillis - lastStartedTimestampMillis > this.plugin.getExecutionIntervalMillis();
        }

        public void setRunning(boolean running) {
            this.running = running;
        }

        public Plugin getPlugin() {
            return plugin;
        }

        @Override
        public void run() {
            this.plugin.run();

        }

    }
}
