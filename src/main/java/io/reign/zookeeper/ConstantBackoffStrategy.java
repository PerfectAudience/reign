package io.reign.zookeeper;

/**
 * 
 * @author ypai
 * 
 */
public class ConstantBackoffStrategy implements BackoffStrategy {
    private final int initial;
    private int currentValue;
    private final int delta;
    private final int max;
    private final boolean loop;

    public ConstantBackoffStrategy(int initial, int delta, int max, boolean loop) {
        this.initial = initial;
        this.currentValue = initial;
        this.delta = delta;
        this.max = max;
        this.loop = loop;
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public Integer next() {
        this.currentValue = this.currentValue + this.delta;
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
