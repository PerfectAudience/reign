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
