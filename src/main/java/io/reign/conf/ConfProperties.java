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

package io.reign.conf;

import java.util.Properties;

/**
 * Functionally the same as java.util.Properties but overrides equals() and hashcode() methods.
 * 
 * Fully synchronized so should not be used in highly concurrent situations.
 * 
 * @author ypai
 * 
 */
public class ConfProperties extends Properties {

    @Override
    public synchronized boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || !(obj instanceof Properties)) {
            return false;
        }

        // compare map sizes
        Properties props = (Properties) obj;
        if (this.size() != props.size()) {
            return false;
        }

        // iterate through map keys and make sure all values are the same
        for (Object objectKey : this.keySet()) {
            String key = (String) objectKey;
            if (!this.getProperty(key).equals(props.getProperty(key))) {
                return false;
            }
        }

        return true;

    }

    @Override
    public synchronized int hashCode() {
        // TODO Auto-generated method stub
        return super.hashCode();
    }

}
