package org.kompany.overlord;

/**
 * For administration of observers.
 * 
 * @author ypai
 * 
 */
public abstract class ServiceObserverWrapper<T> {

    /** cannot be null */
    protected T observer;

    public T getObserver() {
        return observer;
    }

    public void setObserver(T observer) {
        this.observer = observer;
    }

    /**
     * 
     * @param o
     *            updated value; or null if no longer available
     */
    public abstract void notifyObserver(Object o);

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof ServiceObserverWrapper)) {
            return false;
        }
        return this.observer == ((ServiceObserverWrapper) o).getObserver();
    }

    @Override
    public int hashCode() {
        return this.observer.hashCode();
    }

}
