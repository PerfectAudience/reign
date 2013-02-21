package org.kompany.overlord.coord;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkLockWatcher implements Watcher {
    private static final Logger logger = LoggerFactory.getLogger(ZkLockWatcher.class);

    private static AtomicInteger instancesOutstanding = new AtomicInteger(0);

    // private final AtomicInteger watchedReservations = new AtomicInteger(0);
    //
    // private final AtomicInteger watchesSet = new AtomicInteger(0);

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
            logger.debug("LockWatcher created:  instancesOutstanding={}; lockName={}; lockReservation={}",
                    new Object[] { instancesOutstanding.get(), lockPath, lockReservationPath });
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        instancesOutstanding.decrementAndGet();
    }

    // public int incrementWatchesSet() {
    // return this.watchesSet.incrementAndGet();
    // }
    //
    // public int watchesSet() {
    // return this.watchesSet.intValue();
    // }
    //
    // public int incrementWatchedReservations() {
    // return this.watchedReservations.incrementAndGet();
    // }
    //
    // public int decrementWatchedReservations() {
    // return this.watchedReservations.decrementAndGet();
    // }
    //
    // public int watchedReservations() {
    // return this.watchedReservations.intValue();
    // }

    public void waitForEvent(long waitTimeoutMs) throws InterruptedException {
        // if (this.watchedReservations() > 0) {
        synchronized (this) {
            if (waitTimeoutMs == -1) {
                // this.wait();

                long startTimestamp = System.currentTimeMillis();

                // TODO: change this back to above once we fix issue
                // with lost ZK connections not currently notifying
                // LockWatcher(s)
                this.wait(60000);

                if (System.currentTimeMillis() - startTimestamp > 60000) {
                    logger.warn(
                            "Safety wait timeout of 60 seconds reached:  resetting watched items to 0:  instancesOutstanding={}; lockName={}; lockReservation={}",
                            new Object[] { instancesOutstanding.get(), lockPath, lockReservationPath });
                    // this.watchedReservations.set(0);
                }

            } else {
                this.wait(waitTimeoutMs);
            }
        }
        // }// if
    }

    @Override
    public void process(WatchedEvent event) {
        // log if DEBUG
        if (logger.isDebugEnabled()) {
            logger.debug("***** Received ZooKeeper Event:  {}",
                    ReflectionToStringBuilder.toString(event, ToStringStyle.DEFAULT_STYLE));

        }

        // if (this.watchedReservations() == 0) {
        // return;
        //
        // } else {

        // process events
        switch (event.getType()) {
        case NodeCreated:
        case NodeChildrenChanged:
        case NodeDataChanged:
        case NodeDeleted:
            // this.decrementWatchedReservations();
            //
            // if (this.watchedReservations() <= 0) {
            synchronized (this) {
                this.notifyAll();

                if (logger.isDebugEnabled()) {
                    logger.debug("Notifying threads waiting on LockWatcher:  lockWatcher.hashcode()=" + this.hashCode()
                            + "; instancesOutstanding=" + instancesOutstanding.get() + "; lockName=" + lockPath
                            + "; lockReservation=" + lockReservationPath);
                }
            }
            // }
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
