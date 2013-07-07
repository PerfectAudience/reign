package io.reign.data;

/**
 * Queue interface. Each item in the queue can only have 1 value.
 * 
 * @author ypai
 * 
 */
public interface QueueData<V> extends BaseData {

    /**
     * Remove oldest inserted item and return it -- FIFO style
     * 
     * @param <T>
     * @return
     */
    public <T extends V> T pop();

    /**
     * Return oldest inserted item without removing.
     * 
     * @param <T>
     * @return
     */
    public <T extends V> T peek();

    /**
     * Insert item at end of queue
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

    /**
     * 
     * @return max size limit; -1 if no limit
     */
    public int maxSize();

}
