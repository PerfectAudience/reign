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
 * Stack interface. Each item in the stack can only have 1 value.
 * 
 * @author ypai
 * 
 */
public interface StackData<V> extends BaseData {

    /**
     * Remove most recently inserted and return it -- LIFO style
     * 
     * @param <T>
     * @return
     */
    public <T extends V> T pop(Class<T> typeClass);

    /**
     * Return most recently inserted without removing.
     * 
     * @param <T>
     * @return
     */
    public <T extends V> T peek(Class<T> typeClass);

    /**
     * Insert item at top of stack
     * 
     * @param value
     * @return true if successfully pushed; false if error or already at max size limit
     */
    public boolean push(V value);

    /**
     * 
     * @return current number of items
     */
    public int size();

}
