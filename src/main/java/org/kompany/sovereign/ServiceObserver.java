package org.kompany.sovereign;

/**
 * 
 * @author ypai
 * 
 */
public interface ServiceObserver {

    /**
     * Called when the ZooKeeper connection has been recovered. Generally, it would be wise to "reset" the application
     * state as well from a predictable checkpoint: re-establishing locks and other coordination set-pieces.
     * 
     * @param o
     */
    public void stateReset(Object o);

    /**
     * Called when there is a change in ZooKeeper connection status so state is unknown: generally, a signal that your
     * application should go into "safe mode".
     * 
     * @param o
     *            object with some information if applicable; may be null
     */
    public void stateUnknown(Object o);

}
