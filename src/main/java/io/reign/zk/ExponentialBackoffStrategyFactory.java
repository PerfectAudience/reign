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
