package org.kompany.overlord.conf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Properties;

public class PropertiesConfSerializer<T extends Properties> implements ConfSerializer<T> {

    private boolean storeAsXml = false;

    public PropertiesConfSerializer(boolean storeAsXml) {
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
        T conf = (T) new PropertiesConf();
        if (storeAsXml) {
            conf.loadFromXML(in);
        } else {
            conf.load(in);
        }
        return conf;
    }
}
