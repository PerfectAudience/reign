package io.reign.data;

/**
 * Stack interface. Each item in the stack can only have 1 value.
 * 
 * @author ypai
 * 
 */
public interface StackData<V> {

    /**
     * Remove most recently inserted and return it -- LIFO style
     * 
     * @param <T>
     * @return
     */
    public <T extends V> T pop();

    /**
     * Return most recently inserted without removing.
     * 
     * @param <T>
     * @return
     */
    public <T extends V> T peek();

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

    /**
     * 
     * @return max size limit; -1 if no limit
     */
    public int maxSize();

}
