package io.reign.data;

import static org.junit.Assert.assertTrue;
import io.reign.AbstractDataSerializerTest;

import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author ypai
 * 
 */
public class ShortSerializerTest extends AbstractDataSerializerTest {

    private ShortSerializer serializer;

    @Before
    public void setUp() throws Exception {
        serializer = new ShortSerializer();
    }

    @Override
    @Test
    public void testBasic() throws Exception {
        short data1 = Short.MAX_VALUE;
        short data2 = Short.MIN_VALUE;

        byte[] bytes1;
        byte[] bytes2;
        Short reconstituted;

        // serialize and deserialize multiple times to make sure buffers in serializer are correctly manipulated

        bytes1 = serializer.serialize(data1);
        bytes2 = serializer.serialize(data2);

        reconstituted = serializer.deserialize(bytes1);
        assertTrue(data1 + " != " + reconstituted, data1 == reconstituted);

        reconstituted = serializer.deserialize(bytes1);
        assertTrue(data1 + " != " + reconstituted, data1 == reconstituted);

        bytes2 = serializer.serialize(data2);
        reconstituted = serializer.deserialize(bytes2);
        assertTrue(data2 + " != " + reconstituted, data2 == reconstituted);
    }
}
