package io.reign.zk;

import java.util.List;

import org.apache.zookeeper.data.Stat;

public interface PathCacheEntry {
    public long getLastUpdatedTimestampMillis();

    public Stat getStat();

    public byte[] getData();

    public List<String> getChildList();
}
