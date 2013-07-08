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
 * 
 * @author ypai
 * 
 */
public class ZkQueueData<V> implements QueueData<V> {

    private final LinkedListData<V> linkedListData;

    public ZkQueueData(LinkedListData<V> linkedListData) {
        this.linkedListData = linkedListData;
    }

    @Override
    public synchronized void destroy() {
        linkedListData.destroy();
    }

    @Override
    public synchronized V pop() {
        return this.linkedListData.popFirst();
    }

    @Override
    public synchronized V peek() {
        return this.linkedListData.peekFirst();
    }

    @Override
    public synchronized boolean push(V value) {
        return this.linkedListData.pushLast(value);
    }

    @Override
    public synchronized int size() {
        return this.linkedListData.size();
    }

    @Override
    public synchronized int maxSize() {
        return this.linkedListData.maxSize();
    }

}
