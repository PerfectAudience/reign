/*
 Copyright 2013 Yen Pai ypai@reign.io

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package io.reign;

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
