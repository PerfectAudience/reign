/*
 Copyright 2013 Yen Pai ypai@reign.io

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package io.reign.zk;

import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.data.Stat;

/**
 * 
 * @author ypai
 * 
 */
public class SimplePathCacheEntry implements PathCacheEntry {

    private final Stat stat;
    private final byte[] data;
    private final List<String> childList;
    private final long lastUpdatedTimestampMillis;

    public SimplePathCacheEntry(Stat stat, byte[] data, List<String> childList, long lastUpdatedTimestampMillis) {
        this.stat = stat;
        this.data = data;
        this.childList = childList == null || childList.size() == 0 ? Collections.EMPTY_LIST : childList;
        this.lastUpdatedTimestampMillis = lastUpdatedTimestampMillis;
    }

    @Override
    public long getLastUpdatedTimestampMillis() {
        return lastUpdatedTimestampMillis;
    }

    @Override
    public Stat getStat() {
        return stat;
    }

    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public List<String> getChildList() {
        return childList;
    }

}
