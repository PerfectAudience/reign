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
    public <T extends V> T popAt(int index);

    /**
     * Return item at index without removing.
     * 
     * @param <T>
     * @param index
     * @return item at index
     */
    public <T extends V> T peekAt(int index);

    /**
     * Remove first item and return it -- FIFO style
     * 
     * @param <T>
     * @return
     */
    public <T extends V> T popFirst();

    /**
     * Remove last item and return it -- LIFO style
     * 
     * @return
     */
    public <T extends V> T popLast();

    /**
     * Return first item without removing.
     * 
     * @param <T>
     * @return
     */
    public <T extends V> T peekFirst();

    /**
     * Return last item without removing.
     * 
     * @return
     */
    public <T extends V> T peekLast();

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

    /**
     * 
     * @return max size limit; -1 if no limit
     */
    public int maxSize();

}
