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

import io.reign.DataSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Properties;

public class ConfPropertiesSerializer<T extends Properties> implements DataSerializer<T> {

    private boolean storeAsXml = false;

    public ConfPropertiesSerializer(boolean storeAsXml) {
        this.storeAsXml = storeAsXml;
    }

    public boolean isStoreAsXml() {
        return storeAsXml;
    }

    public void setStoreAsXml(boolean storeAsXml) {
        this.storeAsXml = storeAsXml;
    }

    @Override
    public byte[] serialize(T conf) throws RuntimeException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(128);
        try {
            if (storeAsXml) {
                conf.storeToXML(out, null);
            } else {
                conf.store(out, null);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    @Override
    public T deserialize(byte[] bytes) throws RuntimeException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        T conf = (T) new ConfProperties();
        try {
            if (storeAsXml) {
                conf.loadFromXML(in);
            } else {
                conf.load(in);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return conf;
    }
}
