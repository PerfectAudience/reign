package io.reign;

import java.util.Collections;
import java.util.List;

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

public abstract class AbstractObserver implements Observer {

    private String path;
    private byte[] data;
    private List<String> childList = Collections.EMPTY_LIST;

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public void setPath(String path) {
        this.path = path;
    }

    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public void setData(byte[] data) {
        this.data = data;
    }

    @Override
    public List<String> getChildList() {
        return childList;
    }

    @Override
    public void setChildList(List<String> childList) {
        this.childList = childList;
    }

    @Override
    public void stateReset(Object o) {
    }

    @Override
    public void stateUnknown(Object o) {
    }

    @Override
    public void nodeChildrenChanged(List<String> updatedChildList, List<String> previousChildList) {
    }

    @Override
    public void nodeDataChanged(byte[] updatedData, byte[] previousData) {
    }

    @Override
    public void nodeDeleted(byte[] previousData, List<String> previousChildList) {
    }

    @Override
    public void nodeCreated(byte[] data, List<String> childList) {
    }

}
