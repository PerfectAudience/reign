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
    public byte[] serialize(T conf) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream(128);
        if (storeAsXml) {
            conf.storeToXML(out, null);
        } else {
            conf.store(out, null);
        }

        return out.toByteArray();
    }

    @Override
    public T deserialize(byte[] bytes) throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        T conf = (T) new ConfProperties();
        if (storeAsXml) {
            conf.loadFromXML(in);
        } else {
            conf.load(in);
        }
        return conf;
    }
}
