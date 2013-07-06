package io.reign.util.spring;

import io.reign.Reign;
import io.reign.ReignMaker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convenience class to facilitate Spring integration with default core services configured.
 * 
 * @author ypai
 * 
 */
public class SpringReignMaker extends ReignMaker {

    private static final Logger logger = LoggerFactory.getLogger(SpringReignMaker.class);

    private volatile Reign reign;

    public SpringReignMaker() {
        super();
    }

    public void setZkConnectString(String zkConnectString) {
        this.zkConnectString(zkConnectString);
    }

    public void setZkSessionTimeout(int zkSessionTimeout) {
        this.zkSessionTimeout(zkSessionTimeout);
    }

    public void setMessagingPort(Integer messagingPort) {
        this.messagingPort(messagingPort);
    }

    public void setCore(boolean core) {
        if (core) {
            super.core();
        }
    }

    public void setPresence(boolean presence) {
        if (presence) {
            super.presence();
        }
    }

    public void setConf(boolean conf) {
        if (conf) {
            super.conf();
        }
    }

    public void setData(boolean data) {
        if (data) {
            super.data();
        }
    }

    public void setCoord(boolean coord) {
        if (coord) {
            super.coord();
        }
    }

    public void setMesg(boolean mesg) {
        if (mesg) {
            super.mesg();
        }
    }

    @Override
    public Reign get() {
        if (reign == null) {
            synchronized (this) {
                while (reign == null) {
                    try {
                        logger.info("Waiting for initialization...");
                        this.wait(5000);
                    } catch (InterruptedException e) {
                        logger.warn("Interrupted while waiting for initialization", e);
                    }
                } // while
            }
        }
        return reign;
    }

    /**
     * Call using Spring "init-method" when initializing bean. Creates Reign object but does not start.
     */
    public void init() {
        reign = super.get();
        synchronized (this) {
            logger.info("Initialized:  notifying all waiters...");
            this.notifyAll();
        }
    }

    /**
     * Call using Spring "init-method" when initializing bean. Creates Reign object AND starts it up.
     */
    public void initStart() {
        init();
        reign.start();
    }

    /**
     * Call using Spring "init-method" when initializing bean.
     */
    public void destroy() {
        reign.stop();
    }
}
