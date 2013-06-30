package io.reign.zk;

/**
 * 
 * @author ypai
 * 
 */
public class ExponentialBackoffStrategyFactory implements BackoffStrategyFactory {

    private int initial;
    private int max;
    private boolean loop;

    public ExponentialBackoffStrategyFactory(int initial, int max, boolean loop) {
        this.initial = initial;
        this.max = max;
        this.loop = loop;
    }

    @Override
    public BackoffStrategy get() {
        return new ExponentialBackoffStrategy(initial, max, loop);
    }

    public int getInitial() {
        return initial;
    }

    public void setInitial(int initial) {
        this.initial = initial;
    }

    public int getMax() {
        return max;
    }

    public void setMax(int max) {
        this.max = max;
    }

    public boolean isLoop() {
        return loop;
    }

    public void setLoop(boolean loop) {
        this.loop = loop;
    }

}
