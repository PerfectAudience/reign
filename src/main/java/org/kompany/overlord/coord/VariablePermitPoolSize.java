package org.kompany.overlord.coord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class VariablePermitPoolSize implements PermitPoolSize {

    private static final Logger logger = LoggerFactory.getLogger(VariablePermitPoolSize.class);

    private volatile int size;

    public VariablePermitPoolSize(int permitPoolSize) {
        this.size = permitPoolSize;
    }

    @Override
    public int get() {
        return size;
    }

    public void set(int size) {
        this.size = size;
        logger.info("Permit pool size updated:  size={}", size);
    }

}
