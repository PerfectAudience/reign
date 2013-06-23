package io.reign.zookeeper;

/**
 * 
 * @author ypai
 * 
 */
public class ConstantBackoffStrategy implements BackoffStrategy {
    private long initial;
    private long currentValue;
    private long delta;
    private long max;
    private boolean loop;

    public ConstantBackoffStrategy(long initial, long delta, long max,
            boolean loop) {
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
    public Long next() {
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
    public Long get() {
        return this.currentValue;
    }
}
