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

package io.reign.data;

/**
 * A linked list that can be used like a stack or queue. Each item in the list can only have 1 value.
 * 
 * @author ypai
 * 
 */
public interface LinkedListData<V> extends BaseData {

    /**
     * Remove and return item at index.
     * 
     * @param <T>
     * @param index
     * @return item at index
     */
    public <T extends V> T popAt(int index, Class<T> typeClass);

    /**
     * Return item at index without removing.
     * 
     * @param <T>
     * @param index
     * @return item at index
     */
    public <T extends V> T peekAt(int index, Class<T> typeClass);

    /**
     * Remove first item and return it -- FIFO style
     * 
     * @param <T>
     * @return
     */
    public <T extends V> T popFirst(Class<T> typeClass);

    /**
     * Remove last item and return it -- LIFO style
     * 
     * @return
     */
    public <T extends V> T popLast(Class<T> typeClass);

    /**
     * Return first item without removing.
     * 
     * @param <T>
     * @return
     */
    public <T extends V> T peekFirst(Class<T> typeClass);

    /**
     * Return last item without removing.
     * 
     * @return
     */
    public <T extends V> T peekLast(Class<T> typeClass);

    /**
     * Insert item at end of queue
     * 
     * @param value
     * @return true if successfully pushed
     */
    public boolean pushLast(V value);

    /**
     * 
     * @return current number of items
     */
    public int size();

}
