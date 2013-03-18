package org.kompany.sovereign;

/**
 * 
 * @author ypai
 * 
 */
public abstract class AbstractActiveService extends AbstractService implements ActiveService {
    private long executionIntervalMillis = -1;

    @Override
    public abstract void perform();

    public void setExecutionIntervalMillis(long executionIntervalMillis) {
        this.executionIntervalMillis = executionIntervalMillis;
    }

    @Override
    public long getExecutionIntervalMillis() {
        return executionIntervalMillis;
    }

}
