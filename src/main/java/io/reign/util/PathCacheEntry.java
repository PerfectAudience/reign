package io.reign.util;

import java.util.List;

import org.apache.zookeeper.data.Stat;

/**
 * 
 * @author ypai
 * 
 */
public class PathCacheEntry {
    private final Stat stat;
    private final byte[] bytes;
    private final List<String> children;
    private final long lastUpdatedTimestampMillis;

    public PathCacheEntry(Stat stat, byte[] bytes, List<String> children, long lastUpdateTimestampMillis) {
        this.stat = stat;
        this.bytes = bytes;
        this.children = children;
        this.lastUpdatedTimestampMillis = lastUpdateTimestampMillis;
    }

    public long getLastUpdatedTimestampMillis() {
        return lastUpdatedTimestampMillis;
    }

    public Stat getStat() {
        return stat;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public List<String> getChildren() {
        return children;
    }

}
