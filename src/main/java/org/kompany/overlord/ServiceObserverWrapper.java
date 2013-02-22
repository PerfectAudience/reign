package org.kompany.overlord;

/**
 * For administration of observers.
 * 
 * @author ypai
 * 
 * @param <T>
 *            the type observer being wrapped
 */
public abstract class ServiceObserverWrapper<T extends ServiceObserver> {

    /** cannot be null */
    protected T observer;

    public T getObserver() {
        return observer;
    }

    public void setObserver(T observer) {
        this.observer = observer;
    }

    /**
     * Entry point for notifying the wrapped observer of some change.
     * 
     * @param o
     *            updated value; or null if no longer available
     */
    public abstract void signalObserver(Object o);

    /**
     * Overridden to ensure that we do not notify the same observer multiple
     * times.
     */
    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof ServiceObserverWrapper)) {
            return false;
        }
        return this.observer == ((ServiceObserverWrapper) o).getObserver();
    }

    /**
     * Overridden to ensure that we do not notify the same observer multiple
     * times.
     */
    @Override
    public int hashCode() {
        return this.observer.hashCode();
    }

}
