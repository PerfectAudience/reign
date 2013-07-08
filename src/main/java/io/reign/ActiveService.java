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

package io.reign;

/**
 * A service that runs continuously or performs tasks periodically at regular
 * intervals.
 * 
 * @author ypai
 * 
 */
public interface ActiveService extends Service {

    /**
     * 
     * @return 0 if this plug-in is meant to run continuously (framework will
     *         allocate a thread for it); otherwise, the plug-in will share a
     *         thread pool with other plug-ins.
     */
    public long getExecutionIntervalMillis();

    /**
     * Run periodically.
     */
    public void perform();
}
