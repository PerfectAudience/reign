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
