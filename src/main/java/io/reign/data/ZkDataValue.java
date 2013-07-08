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

package io.reign.data;

import org.codehaus.jackson.annotate.JsonIgnore;

/**
 * 
 * @author ypai
 * 
 */
public class ZkDataValue<T> implements DataValue<T> {

    @JsonIgnore
    private String index = DEFAULT_INDEX;

    private final long lastModified;

    private final T value;

    public ZkDataValue(String index, T value, long lastModified) {
        this.index = index;
        this.value = value;
        this.lastModified = lastModified;

    }

    public ZkDataValue(T value, long lastModified) {
        this.value = value;
        this.lastModified = lastModified;

    }

    public ZkDataValue(T value) {
        this.value = value;
        this.lastModified = -1;
    }

    @Override
    public String index() {
        return this.index;
    }

    @Override
    public T value() {
        return value;
    }

    @Override
    public long lastModified() {
        return lastModified;
    }

}
