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

import java.util.List;

/**
 * 
 * @author ypai
 * 
 */
public interface NodeObserver {

    /**
     * Called when the ZooKeeper connection has been recovered. Generally, it would be wise to "reset" the application
     * state as well from a predictable checkpoint: re-establishing locks and other coordination set-pieces.
     * 
     * @param o
     */
    public void stateReset(Object o);

    /**
     * Called when there is a change in ZooKeeper connection status so state is unknown: generally, a signal that your
     * application should go into "safe mode".
     * 
     * @param o
     *            object with some information if applicable; may be null
     */
    public void stateUnknown(Object o);

    public void nodeChildrenChanged(List<String> updatedChildList);

    public void nodeDataChanged(byte[] updatedData);

    public void nodeDeleted();

    public void nodeCreated(byte[] data);

    public String getPath();

    public byte[] getData();

    public List<String> getChildList();

    public void setPath(String path);

    public void setData(byte[] data);

    public void setChildList(List<String> childList);

}
