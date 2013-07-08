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

/**
 * 
 * @author ypai
 * 
 */
public class ConstantBackoffStrategy implements BackoffStrategy {
    private final int initial;
    private int currentValue;
    private final int delta;
    private final int max;
    private final boolean loop;

    public ConstantBackoffStrategy(int initial, int delta, int max, boolean loop) {
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
    public Integer next() {
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
    public Integer get() {
        return this.currentValue;
    }
}
