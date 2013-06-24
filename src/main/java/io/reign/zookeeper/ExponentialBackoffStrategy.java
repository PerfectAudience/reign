package io.reign.zookeeper;

/**
 * 
 * @author ypai
 * 
 */
public class ExponentialBackoffStrategy implements BackoffStrategy {
    private final int initial;
    private int currentValue;
    private final int max;
    private final boolean loop;

    public ExponentialBackoffStrategy(int initial, int max, boolean loop) {
        this.initial = initial;
        this.currentValue = initial;
        this.max = max;
        this.loop = loop;
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public Integer next() {
        this.currentValue = this.currentValue * 2;
        if (this.currentValue > this.max) {
            if (loop) {
                this.currentValue = this.initial;
            } else {
                this.currentValue = this.max;
            }
        }
        return this.currentValue;
    }

    @Override
    public Integer get() {
        return this.currentValue;
    }
}
