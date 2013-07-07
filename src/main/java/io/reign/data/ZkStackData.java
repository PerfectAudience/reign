package io.reign.data;

/**
 * 
 * @author ypai
 * 
 */
public class ZkStackData<V> implements StackData<V> {

    private final LinkedListData<V> linkedListData;

    public ZkStackData(LinkedListData<V> linkedListData) {
        this.linkedListData = linkedListData;
    }

    @Override
    public synchronized void destroy() {
        linkedListData.destroy();
    }

    @Override
    public synchronized V pop() {
        return this.linkedListData.popLast();
    }

    @Override
    public synchronized V peek() {
        return this.linkedListData.peekLast();
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
