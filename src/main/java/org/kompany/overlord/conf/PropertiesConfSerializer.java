package org.kompany.overlord.conf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Properties;

public class PropertiesConfSerializer implements ConfSerializer<Properties> {

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
    public byte[] serialize(Properties conf) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream(128);
        if (storeAsXml) {
            conf.storeToXML(out, null);
        } else {
            conf.store(out, null);
        }

        return out.toByteArray();
    }

    @Override
    public Properties deserialize(byte[] bytes) throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        Properties conf = new Properties();
        if (storeAsXml) {
            conf.loadFromXML(in);
        } else {
            conf.load(in);
        }
        return conf;
    }
}
