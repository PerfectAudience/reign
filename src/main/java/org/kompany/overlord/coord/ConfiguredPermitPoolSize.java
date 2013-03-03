package org.kompany.overlord.coord;

import java.util.Map;

import org.kompany.overlord.conf.ConfObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class ConfiguredPermitPoolSize extends ConfObserver<Map<String, String>> implements PermitPoolSize {

    private static final Logger logger = LoggerFactory.getLogger(ConfiguredPermitPoolSize.class);

    private volatile int size;

    public ConfiguredPermitPoolSize(int permitPoolSize) {
        this.size = permitPoolSize;
    }

    @Override
    public int get() {
        return size;
    }

    @Override
    public void updated(Map<String, String> data) {
        this.size = Integer.parseInt(data.get("permitPoolSize"));
        logger.info("Permit pool size updated:  size={}", size);
    }
}
